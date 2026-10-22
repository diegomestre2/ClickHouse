-- `EXPLAIN PLAN fragments = 1` with `IN` subqueries: every set is a query fragment that fills the set on the initiator,
-- with a `set values` exchange to every fragment that reads it, and the stages of a distributed subquery.

DROP TABLE IF EXISTS small;
DROP TABLE IF EXISTS big;

CREATE TABLE small(sid UInt64, s UInt64) ENGINE = MergeTree ORDER BY sid;
CREATE TABLE big(bid UInt64, b UInt64) ENGINE = MergeTree ORDER BY bid;

INSERT INTO small SELECT number, number % 7 FROM numbers(0, 1000);
INSERT INTO big SELECT number, number % 1000 FROM numbers(0, 100000);

SET explain_query_plan_default = 'legacy';
SET query_plan_join_swap_table = 0;
-- Distributed aggregation cannot enforce a global `max_rows_to_group_by`, so pin it to 0.
SET max_rows_to_group_by = 0;
-- The test harness randomizes these, and aggregation in order changes the plan.
SET distributed_aggregation_memory_efficient = 1, enable_memory_bound_merging_of_aggregation_results = 0, optimize_aggregation_in_order = 0;
-- The test harness randomizes these too: a random join order or probe-row threshold changes or drops the runtime filters.
SET join_runtime_filter_min_probe_rows = 0, query_plan_optimize_join_order_randomize = 0, use_hash_table_stats_for_join_reordering = 0;
-- A set used for the primary key or in `PREWHERE` is built while the query is planned. The cases are about the sets built
-- at execution, except the last one.
SET use_index_for_in_with_subqueries = 0, optimize_move_to_prewhere = 0;

SET
    make_distributed_plan = 1,
    enable_parallel_replicas = 0,
    enable_join_runtime_filters = 0,
    use_statistics = 0,
    distributed_plan_optimize_exchanges = 1,
    distributed_plan_default_shuffle_join_bucket_count = 3,
    distributed_plan_default_reader_bucket_count = 3,
    distributed_plan_force_exchange_kind = 'Streaming';

SELECT '-- IN in WHERE, distributed subquery';
EXPLAIN fragments = 1
SELECT b % 10 AS k, count() FROM big WHERE b IN (SELECT sid FROM small WHERE s = 1) GROUP BY k;

SELECT '-- IN as a value';
-- With `pretty`, the hash key names the set `subquery1` instead of by its hash.
EXPLAIN fragments = 1, pretty = 1
SELECT b IN (SELECT sid FROM small WHERE s = 1) AS f, count() FROM big GROUP BY f;

SELECT '-- one set read by both sides of a join';
EXPLAIN fragments = 1
SELECT count()
FROM big, small
WHERE small.sid = big.b AND big.b IN (SELECT sid FROM small WHERE s = 1) AND small.sid IN (SELECT sid FROM small WHERE s = 1)
SETTINGS distributed_plan_max_rows_to_broadcast = 0;

SELECT '-- IN inside the subquery';
EXPLAIN fragments = 1
SELECT count() FROM big WHERE b IN (SELECT sid FROM small WHERE s IN (SELECT sid FROM small WHERE sid < 3));

SELECT '-- the subquery is not distributed';
EXPLAIN fragments = 1
SELECT count() FROM big WHERE b IN (SELECT sid FROM small GROUP BY sid WITH TOTALS);

SELECT '-- pretty';
EXPLAIN fragments = 1, pretty = 1
SELECT count() FROM big WHERE b IN (SELECT sid FROM small WHERE s = 1);

SELECT '-- a set in PREWHERE is built while the query is planned';
-- The subquery is executed here, with the default exchange kind as in the other tests that execute a distributed plan.
EXPLAIN fragments = 1
SELECT count() FROM big PREWHERE b IN (SELECT sid FROM small WHERE s = 1)
SETTINGS distributed_plan_force_exchange_kind = '';

DROP TABLE big;
DROP TABLE small;
