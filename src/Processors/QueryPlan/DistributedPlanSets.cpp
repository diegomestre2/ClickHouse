#include <Interpreters/Context.h>
#include <Interpreters/PreparedSets.h>
#include <Parsers/IAST.h>
#include <Processors/QueryPlan/CreatingSetsStep.h>
#include <Processors/QueryPlan/DistinctStep.h>
#include <Processors/QueryPlan/DistributedPlanSets.h>
#include <Processors/QueryPlan/GatherReceiveStep.h>
#include <Processors/QueryPlan/MaterializingCTEStep.h>
#include <Processors/QueryPlan/Optimizations/QueryPlanOptimizationSettings.h>
#include <Core/Settings.h>
#include <Common/Exception.h>

namespace DB
{

namespace Setting
{
    extern const SettingsUInt64 max_rows_to_transfer;
}

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace
{

String describeSet(const FutureSetFromSubquery & future_set)
{
    if (auto ast = future_set.getSourceAST())
        return ast->formatForErrorMessage();
    const auto hash = future_set.getHash();
    return fmt::format("with hash {}_{}", hash.low64, hash.high64);
}

}


std::optional<PreformattedMessage> getReasonSetsCannotBeShipped(const DelayedCreatingSetsStep & step)
{
    for (const auto & future_set : step.getSets())
    {
        if (future_set && future_set->hasExternalTable())
            return PreformattedMessage::create(
                "make_distributed_plan does not support sets backed by an external table (`GLOBAL IN` / `GLOBAL JOIN`): "
                "IN-subquery {}",
                describeSet(*future_set));
    }
    return std::nullopt;
}

PreparedSets::Subqueries extractSetsForDistributedPlan(QueryPlan::Node *& root)
{
    PreparedSets::Subqueries sets;

    /// `DelayedCreatingSetsStep` is a transparent placeholder (its output header equals its input
    /// header), so after its sets are taken the node is spliced out of the tree.
    auto take_sets_and_splice_out = [&](QueryPlan::Node *& slot)
    {
        while (auto * delayed = typeid_cast<DelayedCreatingSetsStep *>(slot->step.get()))
        {
            auto step_sets = delayed->detachSets();
            std::move(step_sets.begin(), step_sets.end(), std::back_inserter(sets));
            slot = slot->children.front();
        }
    };

    take_sets_and_splice_out(root);

    std::vector<QueryPlan::Node *> stack;
    stack.push_back(root);
    while (!stack.empty())
    {
        auto * node = stack.back();
        stack.pop_back();
        for (auto & child : node->children)
        {
            take_sets_and_splice_out(child);
            stack.push_back(child);
        }

        /// Some steps own whole subplans (e.g. `ReadFromMerge` children). The root node of such
        /// a subplan cannot be removed from here, so only its sets are taken out; an emptied
        /// `DelayedCreatingSetsStep` does nothing and the serializer skips it.
        for (auto * child_plan : node->step->getChildPlans(/*for_explain=*/ false))
        {
            auto * child_root = child_plan ? child_plan->getRootNode() : nullptr;
            if (!child_root)
                continue;
            if (auto * delayed = typeid_cast<DelayedCreatingSetsStep *>(child_root->step.get()))
            {
                auto step_sets = delayed->detachSets();
                std::move(step_sets.begin(), step_sets.end(), std::back_inserter(sets));
            }
            stack.push_back(child_root);
        }
    }

    return sets;
}

void convertSetSourceForDistributedPlan(QueryPlan & source_plan, const ContextPtr & context)
{
    QueryPlanOptimizationSettings optimization_settings(context);
    if (prepareSetSourceForDistributedPlan(source_plan, context, optimization_settings, /*fallback_reason=*/ nullptr))
        source_plan.convertToDistributed(optimization_settings);
}

bool prepareSetSourceForDistributedPlan(
    QueryPlan & source_plan,
    const ContextPtr & context,
    QueryPlanOptimizationSettings & optimization_settings,
    std::optional<String> * fallback_reason)
{
    /// A source with a step that cannot be serialized for remote execution (e.g. a read from a
    /// table function) builds the set locally on the initiator instead of failing the query.
    if (const auto * node = findNonSerializableStep(source_plan.getRootNode(), /*ignore=*/ {}))
    {
        if (fallback_reason)
            *fallback_reason = fmt::format("step '{}' is not serializable for remote execution", node->step->getName());
        return false;
    }

    /// The row limit makes an over-limit set fail during the build. The byte limit is not
    /// checked here: `DistinctStep` would measure its hash table rather than the shipped
    /// values, so task serialization checks it against the actual columns. A truncated set
    /// would change results, so the mode is throw.
    const auto & settings = context->getSettingsRef();
    /// Default-constructed step settings: this internal `DISTINCT` must not depend on the user's `DISTINCT`
    /// limits or spill to disk; only the transfer limit applies.
    DistinctStep::Settings distinct_settings;
    distinct_settings.set_size_limits = SizeLimits(settings[Setting::max_rows_to_transfer], 0, OverflowMode::THROW);

    auto header = source_plan.getCurrentHeader();
    source_plan.addStep(
        std::make_unique<DistinctStep>(header, std::move(distinct_settings), 0, header->getNames(), /*pre_distinct_=*/false));

    /// An unsupported set source (e.g. `WITH TOTALS` inside the subquery) builds the set locally
    /// on the initiator; the flipped settings also skip the conversion.
    source_plan.applyDistributedPlanFallbackToLocal(optimization_settings);
    source_plan.optimize(optimization_settings);
    if (fallback_reason && source_plan.didFallBackToLocal())
        *fallback_reason = source_plan.getDistributedPlanFallbackReason();
    return optimization_settings.make_distributed_plan;
}

namespace
{

SetForExplain prepareSetForExplain(
    const FutureSetFromSubqueryPtr & future_set, const QueryPlanOptimizationSettings & optimization_settings, const ContextPtr & context)
{
    SetForExplain result;
    result.set = future_set;
    result.description = describeSet(*future_set);
    result.built_during_planning = future_set->get() != nullptr;

    auto source = future_set->detachQueryPlan();
    if (!source || !source->isInitialized())
    {
        /// A set used in `PREWHERE` is built while the plan is optimized, and the build without keeping the values
        /// consumes the plan of the subquery. The set is then only shipped: it has no plan to show.
        if (result.built_during_planning)
            return result;
        throw Exception(ErrorCodes::LOGICAL_ERROR, "The IN-subquery set {} has no plan to build it from", result.description);
    }

    /// Made from the context as `convertSetSourceForDistributedPlan` makes them, with the options of the `EXPLAIN`.
    QueryPlanOptimizationSettings source_settings(context);
    source_settings.is_explain = optimization_settings.is_explain;
    source_settings.keep_logical_steps = optimization_settings.keep_logical_steps;
    source_settings.max_step_description_length = optimization_settings.max_step_description_length;
    if (prepareSetSourceForDistributedPlan(*source, context, source_settings, &result.fallback_reason))
    {
        result.cut = std::make_unique<DistributedPlanCut>(source->cutIntoDistributedPlan(source_settings));
        result.sets = prepareSetsForExplain(std::move(result.cut->delayed_sets), optimization_settings, context);
        result.cut->delayed_sets.clear();

        /// The initiator reads the result of the source from its `final_result` exchange and fills the set from it.
        result.plan.addStep(std::make_unique<GatherReceiveStep>(
            result.cut->result_header, result.cut->final_result_exchange.name, result.cut->final_result_exchange.source_bucket_count));
        future_set->addCreatingSetStep(result.plan, optimization_settings.network_transfer_limits);
        result.cut_source = std::move(source);
        return result;
    }

    /// The source is built locally, as `convertToDistributed` expands the sets it adds back to the initiator plan:
    /// completed with the step that fills the set, and optimized without a distributed plan. Its own sets are expanded
    /// into the plan by that optimization.
    future_set->addCreatingSetStep(*source, optimization_settings.network_transfer_limits);
    removeAllDelayedMaterializingCTEsStep(*source);
    QueryPlanOptimizationSettings local_settings = optimization_settings;
    local_settings.prepared_sets_cache = nullptr;
    local_settings.make_distributed_plan = false;
    source->optimize(local_settings);
    result.plan = std::move(*source);
    return result;
}

}

std::vector<SetForExplain> prepareSetsForExplain(
    std::vector<FutureSetFromSubqueryPtr> sets,
    const QueryPlanOptimizationSettings & optimization_settings,
    const ContextPtr & context)
{
    std::vector<SetForExplain> result;
    result.reserve(sets.size());
    for (const auto & future_set : sets)
    {
        if (!future_set)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "A distributed plan has an empty IN-subquery set");
        /// Not built as a set: the subquery is decorrelated into the outer query (see `prepareForDistributedPlan`).
        if (future_set->hasCorrelatedSource())
            continue;
        result.push_back(prepareSetForExplain(future_set, optimization_settings, context));
    }
    return result;
}

}
