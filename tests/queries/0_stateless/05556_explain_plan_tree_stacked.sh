#!/usr/bin/env bash
# `EXPLAIN PLAN tree = 1` stacks a step with two inputs when its tidy subtree is wider than 120 columns and stacking makes
# it narrower: the wider input goes straight down, the other one to the right of the step. A chain of joins is stacked,
# a fan-out (a `Union` of many reads) is not. The queries are only explained, never executed.

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
-- A read box shows its parts and granules: pin the granularity and the part format, which the test harness randomizes.
CREATE TABLE $DB.orders(order_id UInt64, customer_id UInt64, product_id UInt64) ENGINE = MergeTree ORDER BY order_id SETTINGS index_granularity = 8192, index_granularity_bytes = '10Mi', min_bytes_for_wide_part = 0;
CREATE TABLE $DB.customers(customer_id UInt64, city_id UInt64) ENGINE = MergeTree ORDER BY customer_id SETTINGS index_granularity = 8192, index_granularity_bytes = '10Mi', min_bytes_for_wide_part = 0;
CREATE TABLE $DB.cities(city_id UInt64, country_id UInt64, city_name_in_english String) ENGINE = MergeTree ORDER BY city_id SETTINGS index_granularity = 8192, index_granularity_bytes = '10Mi', min_bytes_for_wide_part = 0;
CREATE TABLE $DB.countries(country_id UInt64, region_id UInt64) ENGINE = MergeTree ORDER BY country_id SETTINGS index_granularity = 8192, index_granularity_bytes = '10Mi', min_bytes_for_wide_part = 0;
CREATE TABLE $DB.regions(region_id UInt64, name String) ENGINE = MergeTree ORDER BY region_id SETTINGS index_granularity = 8192, index_granularity_bytes = '10Mi', min_bytes_for_wide_part = 0;
CREATE TABLE $DB.products(product_id UInt64, price UInt64) ENGINE = MergeTree ORDER BY product_id SETTINGS index_granularity = 8192, index_granularity_bytes = '10Mi', min_bytes_for_wide_part = 0;

INSERT INTO $DB.orders SELECT number, number % 1000, number % 100 FROM numbers(100000);
OPTIMIZE TABLE $DB.orders FINAL;
INSERT INTO $DB.customers SELECT number, number % 100 FROM numbers(1000);
INSERT INTO $DB.cities SELECT number, number % 10, toString(number) FROM numbers(100);
INSERT INTO $DB.countries SELECT number, number % 3 FROM numbers(10);
INSERT INTO $DB.regions SELECT number, toString(number) FROM numbers(3);
INSERT INTO $DB.products SELECT number, number FROM numbers(100);

SET query_plan_join_swap_table = 0;
-- Distributed aggregation cannot enforce a global \`max_rows_to_group_by\`, so pin it to 0.
SET max_rows_to_group_by = 0;
-- The test harness randomizes these, and aggregation in order changes the plan.
SET distributed_aggregation_memory_efficient = 1, enable_memory_bound_merging_of_aggregation_results = 0, optimize_aggregation_in_order = 0;
-- The test harness randomizes these, and they change the join order and so the plan.
SET join_runtime_filter_min_probe_rows = 0, query_plan_optimize_join_order_randomize = 0, use_hash_table_stats_for_join_reordering = 0;
-- Without join reordering the joins keep the order of the query, a chain whose left input is the previous join. The
-- layout switches at 120 columns, so every join of the chain is far from it: the first one is about 80 columns wide, and
-- the long column name \`city_name_in_english\` widens the read of \`cities\` so that the second one is about 140 columns
-- wide before it is stacked.
SET query_plan_optimize_join_order_limit = 0;
SET enable_parallel_replicas = 0, enable_join_runtime_filters = 0, use_statistics = 0;

SELECT '-- join chain, single node';
EXPLAIN tree = 1
SELECT r.name, any(ci.city_name_in_english), sum(p.price)
FROM $DB.orders AS o
JOIN $DB.customers AS c ON o.customer_id = c.customer_id
JOIN $DB.cities AS ci ON c.city_id = ci.city_id
JOIN $DB.countries AS co ON ci.country_id = co.country_id
JOIN $DB.regions AS r ON co.region_id = r.region_id
JOIN $DB.products AS p ON o.product_id = p.product_id
GROUP BY r.name;

-- Wider than 120 columns, but every read is in its own columns either way: not stacked.
SELECT '-- union of wide reads';
EXPLAIN tree = 1
SELECT customer_id FROM $DB.orders
UNION ALL SELECT customer_id FROM $DB.customers
UNION ALL SELECT city_id FROM $DB.cities
UNION ALL SELECT country_id FROM $DB.countries;

-- In ClickHouse Cloud a scan on shared storage is replaced by \`ReadFromMergeTreeAtWorker\`, which prints no table name.
SET distributed_plan_prefer_replicas_over_workers = 1;

SET
    make_distributed_plan = 1,
    distributed_plan_optimize_exchanges = 1,
    distributed_plan_default_shuffle_join_bucket_count = 3,
    distributed_plan_default_reader_bucket_count = 3,
    distributed_plan_force_exchange_kind = 'Streaming',
    -- These queries must be distributed: a fallback to local execution is an error, not a plain plan.
    distributed_plan_fallback_to_local_execution = 0;

-- Every join is a fragment that reads two shuffles. The receive step of the right input is drawn right of the join, and
-- the fragment it reads is drawn before the fragment read by the left input, whose edge passes it on the left.
SELECT '-- join chain, stateless workers';
EXPLAIN tree = 1
SELECT r.name, any(ci.city_name_in_english), sum(p.price)
FROM $DB.orders AS o
JOIN $DB.customers AS c ON o.customer_id = c.customer_id
JOIN $DB.cities AS ci ON c.city_id = ci.city_id
JOIN $DB.countries AS co ON ci.country_id = co.country_id
JOIN $DB.regions AS r ON co.region_id = r.region_id
JOIN $DB.products AS p ON o.product_id = p.product_id
GROUP BY r.name
SETTINGS distributed_plan_max_rows_to_broadcast = 0;
EOF

$CLICKHOUSE_CLIENT -q "DROP DATABASE $DB"
