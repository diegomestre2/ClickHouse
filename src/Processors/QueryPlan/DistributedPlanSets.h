#pragma once

#include <Interpreters/Context_fwd.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Common/LoggingFormatStringHelpers.h>

#include <memory>
#include <optional>
#include <vector>

namespace DB
{

class FutureSetFromSubquery;
using FutureSetFromSubqueryPtr = std::shared_ptr<FutureSetFromSubquery>;
class DelayedCreatingSetsStep;
struct QueryPlanOptimizationSettings;

/// Handling of `IN`-subquery sets for a distributed query plan (`make_distributed_plan`).
/// The sets are built once on the initiator and their values ship with the worker tasks;
/// see `QueryPlan::convertToDistributed` for how these functions are used around the cut.

/// The reason the sets of this step cannot be shipped with the worker tasks, or nullopt: a set
/// backed by an external table (`GLOBAL IN` / `GLOBAL JOIN` next to a classic remote read) lives in
/// the initiator's session as a temporary table, and a task carries set values, not tables. Called
/// from the pre-optimization decision, so no such set is built for a query that is rejected.
std::optional<PreformattedMessage> getReasonSetsCannotBeShipped(const DelayedCreatingSetsStep & step);

/// Detaches the IN-subquery sets from the delayed set steps and removes those steps from the
/// plan, so the fragments never carry them; the caller re-adds the sets to the initiator plan.
std::vector<FutureSetFromSubqueryPtr> extractSetsForDistributedPlan(QueryPlan::Node *& root);

/// Makes a set subquery source run as a distributed plan: the values are deduplicated on the
/// workers (bounded by `max_rows_to_transfer`, so an over-limit set fails during the build)
/// while the set itself is still filled on the initiator. A source that collapses to a single
/// stage, or that has a step that cannot be serialized, keeps running locally.
void convertSetSourceForDistributedPlan(QueryPlan & source_plan, const ContextPtr & context);

/// The first part of `convertSetSourceForDistributedPlan`: appends the `DISTINCT`, makes the
/// distributed-plan decision for the source and optimizes it. Returns true when the source runs as
/// a distributed plan; `optimization_settings`, made from `context` by the caller, are then the
/// settings to convert or cut it with. Otherwise the set is built from a local plan, and
/// `fallback_reason`, when given, is set to the reason the source was rejected, if it was.
bool prepareSetSourceForDistributedPlan(
    QueryPlan & source_plan,
    const ContextPtr & context,
    QueryPlanOptimizationSettings & optimization_settings,
    std::optional<String> * fallback_reason);

/// An `IN`-subquery set of a distributed plan as `EXPLAIN` shows it: its source is prepared and cut
/// as the execution of the query would do it, but the set is not built.
struct SetForExplain
{
    FutureSetFromSubqueryPtr set;
    /// The subquery the set is filled from, e.g. `(SELECT k FROM small)`.
    String description;
    /// The plan of the fragment that fills the set on the initiator: `CreatingSet` over the receive of the result
    /// of `cut` when the source is distributed, or over the whole source plan when it is not. Empty for a set built
    /// while the query was planned by a build that consumed the plan of the subquery.
    QueryPlan plan;
    /// The source is distributed: its stages. `cut->delayed_sets` is empty; its sets are in `sets`.
    std::unique_ptr<DistributedPlanCut> cut;
    /// The source plan the cut consumed: empty, it keeps the resources of the stages (e.g. the locks of the tables).
    std::unique_ptr<QueryPlan> cut_source;
    /// The source is not distributed: why it was rejected, if it was.
    std::optional<String> fallback_reason;
    /// The set was already filled while the query was planned (e.g. for the analysis of a primary key),
    /// so the execution of the query does not fill it again.
    bool built_during_planning = false;
    /// The sets of a distributed source.
    std::vector<SetForExplain> sets;
};

/// Prepares the sets taken out of a distributed plan by `QueryPlan::cutIntoDistributedPlan` for `EXPLAIN`,
/// recursively for the sets of their sources. Consumes the source plans of the sets: the sets cannot be built
/// afterwards. A set whose source has correlated expressions is not built as a set at all, so it is left out.
std::vector<SetForExplain> prepareSetsForExplain(
    std::vector<FutureSetFromSubqueryPtr> sets,
    const QueryPlanOptimizationSettings & optimization_settings,
    const ContextPtr & context);

}
