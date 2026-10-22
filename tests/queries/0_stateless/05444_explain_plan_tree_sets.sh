#!/usr/bin/env bash
# `EXPLAIN PLAN tree = 1` with `IN` subqueries: every set is drawn to the right of the fragment that reads it, with an
# edge from the fragment that fills the set into the step that reads it.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The widths of the boxes are computed from the step texts, and the text of a read step carries the database name.
# The harness replaces the name of the test database with `default` after the run, which would break the widths, so
# the tables live in a database whose name has a fixed length and is printed as a placeholder of the same length.
DB="db_$(echo -n "$CLICKHOUSE_DATABASE" | md5sum | cut -c1-8)"
PLACEHOLDER="db_XXXXXXXX"

$CLICKHOUSE_CLIENT -q "DROP DATABASE IF EXISTS $DB"
$CLICKHOUSE_CLIENT -q "CREATE DATABASE $DB"

$CLICKHOUSE_CLIENT --multiquery <<EOF | sed "s/$DB/$PLACEHOLDER/g"
CREATE TABLE $DB.small(sid UInt64, s UInt64) ENGINE = MergeTree ORDER BY sid;
CREATE TABLE $DB.big(bid UInt64, b UInt64) ENGINE = MergeTree ORDER BY bid;

INSERT INTO $DB.small SELECT number, number % 7 FROM numbers(0, 1000);
INSERT INTO $DB.big SELECT number, number % 1000 FROM numbers(0, 100000);

SET query_plan_join_swap_table = 0;
-- Distributed aggregation cannot enforce a global \`max_rows_to_group_by\`, so pin it to 0.
SET max_rows_to_group_by = 0;
-- The test harness randomizes these, and aggregation in order changes the plan.
SET distributed_aggregation_memory_efficient = 1, enable_memory_bound_merging_of_aggregation_results = 0, optimize_aggregation_in_order = 0;
-- A set used for the primary key or in \`PREWHERE\` is built while the query is planned. The cases are about the sets built
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

SELECT '-- IN in WHERE';
EXPLAIN tree = 1
SELECT b % 10 AS k, count() FROM $DB.big WHERE b IN (SELECT sid FROM $DB.small WHERE s = 1) GROUP BY k;

SELECT '-- two sets read by one step';
EXPLAIN tree = 1
SELECT count() FROM $DB.big WHERE b IN (SELECT sid FROM $DB.small WHERE s = 1) AND bid IN (SELECT sid FROM $DB.small WHERE s = 2);

SELECT '-- two sets read by different steps';
EXPLAIN tree = 1
SELECT b IN (SELECT sid FROM $DB.small WHERE s = 1) AS f, count() FROM $DB.big WHERE bid IN (SELECT sid FROM $DB.small WHERE s = 2) GROUP BY f;

SELECT '-- one set read by both sides of a join';
EXPLAIN tree = 1
SELECT count()
FROM $DB.big AS big, $DB.small AS small
WHERE small.sid = big.b AND big.b IN (SELECT sid FROM $DB.small WHERE s = 1) AND small.sid IN (SELECT sid FROM $DB.small WHERE s = 1)
SETTINGS distributed_plan_max_rows_to_broadcast = 0;

SELECT '-- IN inside the subquery';
EXPLAIN tree = 1
SELECT count() FROM $DB.big WHERE b IN (SELECT sid FROM $DB.small WHERE s IN (SELECT sid FROM $DB.small WHERE sid < 3));

SELECT '-- the subquery is not distributed';
EXPLAIN tree = 1
SELECT count() FROM $DB.big WHERE b IN (SELECT sid FROM $DB.small GROUP BY sid WITH TOTALS);

SELECT '-- without pretty names';
EXPLAIN tree = 1, pretty = 0
SELECT count() FROM $DB.big WHERE b IN (SELECT sid FROM $DB.small WHERE s = 1);

SELECT '-- a set in PREWHERE is built while the query is planned';
-- The subquery is executed here, with the default exchange kind as in the other tests that execute a distributed plan.
EXPLAIN tree = 1
SELECT count() FROM $DB.big PREWHERE b IN (SELECT sid FROM $DB.small WHERE s = 1)
SETTINGS distributed_plan_force_exchange_kind = '';
EOF

$CLICKHOUSE_CLIENT -q "DROP DATABASE $DB"
