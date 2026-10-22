#include <Processors/QueryPlan/Fragments/FragmentGraph.h>

#include <Interpreters/PreparedSets.h>
#include <Processors/QueryPlan/BroadcastReceiveStep.h>
#include <Processors/QueryPlan/BroadcastSendStep.h>
#include <Processors/QueryPlan/GatherReceiveStep.h>
#include <Processors/QueryPlan/GatherSendStep.h>
#include <Processors/QueryPlan/ShuffleReceiveStep.h>
#include <Processors/QueryPlan/ShuffleSendStep.h>
#include <Common/Exception.h>
#include <Common/typeid_cast.h>

#include <fmt/format.h>

#include <algorithm>
#include <cmath>
#include <unordered_set>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace
{

constexpr std::string_view main_stage_name = "main";

const DistributedQueryStage & findStage(const DistributedQueryPlan & distributed_plan, const String & stage_name)
{
    auto it = distributed_plan.stages.find(stage_name);
    if (it == distributed_plan.stages.end())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Stage {} is referenced by a dependency, but does not exist", stage_name);
    return it->second;
}

const ExchangeDescription & findDescription(const DistributedQueryPlan & distributed_plan, const String & exchange_name)
{
    auto it = distributed_plan.exchange_descriptions.find(exchange_name);
    if (it == distributed_plan.exchange_descriptions.end())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Exchange {} has no description", exchange_name);
    return it->second;
}

bool isExchangeEndpointStep(const IQueryPlanStep & step)
{
    return typeid_cast<const ShuffleSendStep *>(&step) || typeid_cast<const GatherSendStep *>(&step)
        || typeid_cast<const BroadcastSendStep *>(&step) || typeid_cast<const ShuffleReceiveStep *>(&step)
        || typeid_cast<const GatherReceiveStep *>(&step) || typeid_cast<const BroadcastReceiveStep *>(&step);
}

/// A fragment is a stage of the executor, so it always has a plan.
void checkNotEmpty(const QueryPlan & plan, const String & stage_name)
{
    if (!plan.getRootNode())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "The plan of stage {} is empty", stage_name);
}

/// The estimated number of rows a producer fragment sends: the estimate of its root node,
/// or of the input of its send step, because the send step is added by the cutter without an estimate.
std::optional<UInt64> getEstimatedRows(const QueryPlan & plan)
{
    const auto * node = plan.getRootNode();
    if (!node)
        return std::nullopt;
    if (!node->cost_estimation && isExchangeEndpointStep(*node->step) && node->children.size() == 1)
        node = node->children.front();
    if (!node->cost_estimation)
        return std::nullopt;
    return static_cast<UInt64>(std::llround(std::max(0.0, node->cost_estimation->rows)));
}

FragmentExchange::Kind getExchangeKind(const ExchangeDescription & description)
{
    if (description.exchange_step_name == "ShuffleExchange" || description.exchange_step_name == "ScatterExchange")
        return description.keys.empty() ? FragmentExchange::Kind::Any : FragmentExchange::Kind::Hash;
    if (description.exchange_step_name == "BroadcastExchange")
        return FragmentExchange::Kind::Broadcast;
    if (description.exchange_step_name == "GatherExchange")
        return FragmentExchange::Kind::Gather;
    throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected exchange step '{}' of exchange {}",
        description.exchange_step_name, description.name);
}

}

const QueryFragment & FragmentGraph::getFragment(UInt32 id) const
{
    auto it = std::lower_bound(fragments.begin(), fragments.end(), id, [](const QueryFragment & f, UInt32 value) { return f.id < value; });
    if (it == fragments.end() || it->id != id)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Fragment {} is not found in the fragment graph", id);
    return *it;
}

const FragmentExchange & FragmentGraph::getExchange(UInt32 id) const
{
    auto it = std::lower_bound(exchanges.begin(), exchanges.end(), id, [](const FragmentExchange & e, UInt32 value) { return e.id < value; });
    if (it == exchanges.end() || it->id != id)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Exchange {} is not found in the fragment graph", id);
    return *it;
}

namespace
{

/// The indexes of the fragments of a valid graph in a topological order, producers first, and the producers of each.
struct TopologicalOrder
{
    std::vector<size_t> order;
    /// Fragment index -> the indexes of the fragments it reads from.
    std::vector<std::vector<size_t>> producers;
};

/// The index of the fragment with the given id in `fragments`, which is sorted by id; nullopt when absent.
std::optional<size_t> findFragmentIndex(const FragmentGraph & graph, UInt32 id)
{
    auto it = std::lower_bound(graph.fragments.begin(), graph.fragments.end(), id,
        [](const QueryFragment & f, UInt32 value) { return f.id < value; });
    if (it == graph.fragments.end() || it->id != id)
        return std::nullopt;
    return it - graph.fragments.begin();
}

/// Validates the graph (see `validateFragmentGraph`) and sorts it producers first, as `computeLevels` needs. The sort
/// detects cycles. The ids assigned by the cutter mostly ascend from producers to consumers, but the order of ids is
/// not topological, because the fragments of `IN`-subquery sets get ids after the fragments that read them.
TopologicalOrder sortTopologically(const FragmentGraph & graph)
{
    const size_t num_fragments = graph.fragments.size();

    /// Strictly ascending ids: unique, and sorted as the binary searches of `getFragment` and `getExchange` need.
    for (size_t i = 1; i < num_fragments; ++i)
        if (graph.fragments[i - 1].id >= graph.fragments[i].id)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Fragments {} (id {}) and {} (id {}) are not in strictly ascending order of ids",
                graph.fragments[i - 1].stage_name, graph.fragments[i - 1].id, graph.fragments[i].stage_name, graph.fragments[i].id);
    for (size_t i = 1; i < graph.exchanges.size(); ++i)
        if (graph.exchanges[i - 1].id >= graph.exchanges[i].id)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Exchanges {} (id {}) and {} (id {}) are not in strictly ascending order of ids",
                graph.exchanges[i - 1].name, graph.exchanges[i - 1].id, graph.exchanges[i].name, graph.exchanges[i].id);

    TopologicalOrder result;
    result.producers.resize(num_fragments);
    std::vector<size_t> in_degree(num_fragments, 0);
    std::vector<std::vector<size_t>> consumers(num_fragments);
    for (const auto & exchange : graph.exchanges)
    {
        auto producer = findFragmentIndex(graph, exchange.from);
        if (!producer)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Exchange {} comes from unknown fragment {}", exchange.name, exchange.from);
        auto consumer = findFragmentIndex(graph, exchange.to);
        if (!consumer)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Exchange {} goes to unknown fragment {}", exchange.name, exchange.to);
        consumers[*producer].push_back(*consumer);
        result.producers[*consumer].push_back(*producer);
        ++in_degree[*consumer];
    }

    /// Kahn's algorithm, with `order` as the queue of fragments whose producers are all taken. The fragments of a
    /// cycle never become ready, so `order` ends up short.
    result.order.reserve(num_fragments);
    for (size_t i = 0; i < num_fragments; ++i)
        if (in_degree[i] == 0)
            result.order.push_back(i);

    for (size_t pos = 0; pos < result.order.size(); ++pos)
        for (size_t consumer : consumers[result.order[pos]])
            if (--in_degree[consumer] == 0)
                result.order.push_back(consumer);

    if (result.order.size() != num_fragments)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "The fragment graph has a cycle");

    return result;
}

}

void validateFragmentGraph(const FragmentGraph & graph)
{
    sortTopologically(graph);
}

std::vector<std::vector<UInt32>> computeLevels(const FragmentGraph & graph)
{
    const auto sorted = sortTopologically(graph);

    /// A fragment is one level above its deepest producer.
    std::vector<size_t> level_of(graph.fragments.size(), 0);
    size_t max_level = 0;
    for (size_t i : sorted.order)
    {
        for (size_t producer : sorted.producers[i])
            level_of[i] = std::max(level_of[i], level_of[producer] + 1);
        max_level = std::max(max_level, level_of[i]);
    }

    /// The fragments are sorted by id, so every level comes out ascending.
    std::vector<std::vector<UInt32>> levels(max_level + 1);
    for (size_t i = 0; i < graph.fragments.size(); ++i)
        levels[level_of[i]].push_back(graph.fragments[i].id);
    return levels;
}

namespace
{

/// Builds a graph from the cut of the query and from the cuts of the sources of its sets, which number their stages and
/// exchanges from 0 each: the ids of every cut after the first are shifted past the ids taken so far.
class FragmentGraphBuilder
{
public:
    FragmentGraph build(const DistributedQueryPlan & distributed_plan, const std::vector<SetForExplain> & sets)
    {
        auto readers = addCut(distributed_plan, /*set_fragment=*/ std::nullopt);
        addSets(sets, readers);

        /// The fragment that fills a set gets its id after the stages of its source, but is added before them.
        std::sort(graph.fragments.begin(), graph.fragments.end(), [](const auto & lhs, const auto & rhs) { return lhs.id < rhs.id; });
        std::sort(graph.exchanges.begin(), graph.exchanges.end(), [](const auto & lhs, const auto & rhs) { return lhs.id < rhs.id; });
        validateFragmentGraph(graph);
        return std::move(graph);
    }

private:
    FragmentGraph graph;
    UInt32 fragment_offset = 0;
    UInt32 exchange_offset = 0;
    size_t num_sets = 0;

    /// The fragments of one cut that may read the sets taken out of it, by id, with the plan of each.
    using Readers = std::vector<std::pair<UInt32, const QueryPlan *>>;

    static UInt32 getMainStageId(const DistributedQueryPlan & distributed_plan)
    {
        return findStage(distributed_plan, String(main_stage_name)).id;
    }

    /// Adds the stages of a cut and the exchanges between them, with the ids shifted by the current offsets, and moves
    /// the offsets past the ids of the cut. A stage id is at most the id of `main`, and an exchange id is the id of
    /// the stage that sends to it.
    Readers addCut(const DistributedQueryPlan & distributed_plan, std::optional<UInt32> set_fragment)
    {
        const UInt32 main_id = getMainStageId(distributed_plan);
        Readers readers;

        /// The graph is the data flow of the query. The merge stages of runtime filters (`filter_only`, added by
        /// `wireRuntimeFilterExchangeTopology`) produce no query data and are not cut from the plan, so they and the
        /// dependencies on either side of them are left out. The steps that build and apply the filters stay in the
        /// plans of the data fragments.
        for (const auto & [stage_name, stage] : distributed_plan.stages)
        {
            if (stage.filter_only)
                continue;

            checkNotEmpty(stage.query_plan_fragment, stage_name);
            graph.fragments.push_back(QueryFragment{
                .id = fragment_offset + stage.id,
                .stage_name = stage_name,
                .execution_mode = QueryFragment::ExecutionMode::Distributed,
                .fallback_reason = {},
                .description = {},
                .set_name = {},
                .set_fragment = set_fragment,
                .built_during_planning = false,
                .tasks = stage.tasks.size(),
                .plan = &stage.query_plan_fragment,
            });
            readers.emplace_back(fragment_offset + stage.id, &stage.query_plan_fragment);
        }
        /// `stages` is an unordered map: sort for a deterministic order of the `SetValues` exchanges.
        std::sort(readers.begin(), readers.end());

        for (const auto & [consumer_stage, producers] : distributed_plan.stage_depends_on)
        {
            const auto & consumer_stage_description = findStage(distributed_plan, consumer_stage);
            if (consumer_stage_description.filter_only)
                continue;
            for (const auto & [producer_stage, exchange_name] : producers)
            {
                const auto & producer_stage_description = findStage(distributed_plan, producer_stage);
                if (producer_stage_description.filter_only)
                    continue;
                const auto & description = findDescription(distributed_plan, exchange_name);
                const auto kind = getExchangeKind(description);

                graph.exchanges.push_back(FragmentExchange{
                    .id = exchange_offset + description.id,
                    .name = description.name,
                    .set_fragment = set_fragment,
                    .from = fragment_offset + producer_stage_description.id,
                    .to = fragment_offset + consumer_stage_description.id,
                    .kind = kind,
                    .keys = kind == FragmentExchange::Kind::Hash ? description.keys : Names{},
                    .merge_sort = kind == FragmentExchange::Kind::Gather ? description.sort_description : std::nullopt,
                    .mode = description.kind,
                    .source_buckets = description.source_bucket_count,
                    .destination_buckets = description.destination_bucket_count,
                    .statistics = {.estimated_rows = getEstimatedRows(producer_stage_description.query_plan_fragment)},
                });
            }
        }

        fragment_offset += main_id + 1;
        exchange_offset += main_id + 1;
        return readers;
    }

    void addSets(const std::vector<SetForExplain> & sets, const Readers & readers)
    {
        if (sets.empty())
            return;

        /// The sets that the tasks of each reader carry, by hash.
        std::vector<std::unordered_set<FutureSet::Hash, FutureSetHashing>> sets_of_readers;
        sets_of_readers.reserve(readers.size());
        for (const auto & [id, plan] : readers)
        {
            auto & reader_sets = sets_of_readers.emplace_back();
            for (const auto & future_set : plan->collectSerializedSets())
                if (typeid_cast<const FutureSetFromSubquery *>(future_set.get()))
                    reader_sets.insert(future_set->getHash());
        }

        /// Every set a reader carries must be filled by one of the fragments: a missing one would be an edge silently
        /// left out of the graph.
        std::unordered_set<FutureSet::Hash, FutureSetHashing> known_sets;
        for (const auto & set : sets)
            known_sets.insert(set.set->getHash());
        for (size_t i = 0; i < readers.size(); ++i)
            for (const auto & hash : sets_of_readers[i])
                if (!known_sets.contains(hash))
                    throw Exception(ErrorCodes::LOGICAL_ERROR,
                        "Fragment {} carries an IN-subquery set with hash {}_{}, which no fragment of the graph fills",
                        readers[i].first, hash.low64, hash.high64);

        for (const auto & set : sets)
        {
            const String name = fmt::format("set_{}", num_sets++);

            /// The fragment that fills the set follows the stages of its source, if the source is distributed.
            const UInt32 set_fragment_id = fragment_offset + (set.cut ? getMainStageId(set.cut->distributed_plan) + 1 : 0);
            graph.fragments.push_back(QueryFragment{
                .id = set_fragment_id,
                .stage_name = name,
                .execution_mode = QueryFragment::ExecutionMode::SingleNode,
                .fallback_reason = set.fallback_reason,
                .description = set.description,
                .set_name = PreparedSets::toString(set.set->getHash(), {}),
                .set_fragment = {},
                .built_during_planning = set.built_during_planning,
                .tasks = 1,
                .plan = set.plan.isInitialized() ? &set.plan : nullptr,
            });

            const auto hash = set.set->getHash();
            for (size_t i = 0; i < readers.size(); ++i)
            {
                if (!sets_of_readers[i].contains(hash))
                    continue;
                graph.exchanges.push_back(FragmentExchange{
                    .id = exchange_offset++,
                    .name = name,
                    .set_fragment = {},
                    .from = set_fragment_id,
                    .to = readers[i].first,
                    .kind = FragmentExchange::Kind::SetValues,
                    .keys = {},
                    .merge_sort = {},
                    .mode = ExchangeDescription::Kind::Persisted,
                    .source_buckets = 0,
                    .destination_buckets = 0,
                    .statistics = {},
                });
            }

            if (!set.cut)
            {
                fragment_offset = set_fragment_id + 1;
                continue;
            }

            const auto & set_cut = *set.cut;
            const UInt32 source_fragment_offset = fragment_offset;
            const UInt32 source_exchange_offset = exchange_offset;
            auto source_readers = addCut(set_cut.distributed_plan, set_fragment_id);

            /// The `main` stage of the source sends its result to the initiator, which fills the set from it.
            const UInt32 main_id = getMainStageId(set_cut.distributed_plan);
            const auto & final_result = set_cut.final_result_exchange;
            graph.exchanges.push_back(FragmentExchange{
                .id = source_exchange_offset + final_result.id,
                .name = final_result.name,
                .set_fragment = set_fragment_id,
                .from = source_fragment_offset + main_id,
                .to = set_fragment_id,
                .kind = FragmentExchange::Kind::Gather,
                .keys = {},
                .merge_sort = {},
                .mode = final_result.kind,
                .source_buckets = final_result.source_bucket_count,
                .destination_buckets = final_result.destination_bucket_count,
                .statistics = {.estimated_rows = getEstimatedRows(findStage(set_cut.distributed_plan, String(main_stage_name)).query_plan_fragment)},
            });

            /// `addCut` moved the offset past `main`, which is where the fragment that fills the set is.
            fragment_offset = set_fragment_id + 1;
            addSets(set.sets, source_readers);
        }
    }
};

}

FragmentGraph buildFragmentGraph(const DistributedQueryPlan & distributed_plan)
{
    return FragmentGraphBuilder().build(distributed_plan, /*sets=*/ {});
}

FragmentGraph buildFragmentGraph(const DistributedPlanCut & cut, const std::vector<SetForExplain> & sets)
{
    return FragmentGraphBuilder().build(cut.distributed_plan, sets);
}

FragmentGraph buildSingleFragmentGraph(const QueryPlan & plan, std::optional<String> fallback_reason)
{
    FragmentGraph graph;

    checkNotEmpty(plan, String(main_stage_name));
    graph.fragments.push_back(QueryFragment{
        .id = 0,
        .stage_name = String(main_stage_name),
        .execution_mode = QueryFragment::ExecutionMode::SingleNode,
        .fallback_reason = std::move(fallback_reason),
        .description = {},
        .set_name = {},
        .set_fragment = {},
        .built_during_planning = false,
        .tasks = 1,
        .plan = &plan,
    });
    return graph;
}

}
