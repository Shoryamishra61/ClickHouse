-- The rule-based distributed planner copies the right side of a broadcast join, and a table it
-- keeps as one read task, to every node. Both used to pass on a row count alone; now they also have
-- to fit `distributed_plan_max_bytes_to_broadcast`, modeled as rows times the average row width,
-- and a side without a row estimate may pass on its proven row bound.
SET enable_analyzer = 1;
SET enable_parallel_replicas = 0;
SET explain_query_plan_default = 'legacy';
SET max_rows_to_group_by = 0;
SET make_distributed_plan = 1;
SET enable_cascades_optimizer = 0;
SET enable_join_runtime_filters = 0;
SET query_plan_join_swap_table = 0;
SET query_plan_optimize_join_order_randomize = 0;
SET distributed_plan_default_shuffle_join_bucket_count = 2;
SET distributed_plan_default_reader_bucket_count = 2;
SET distributed_plan_optimize_exchanges = 1;
SET use_statistics = 0;

DROP TABLE IF EXISTS t_rb_small;
DROP TABLE IF EXISTS t_rb_big;
CREATE TABLE t_rb_small (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k SETTINGS auto_statistics_types = '';
CREATE TABLE t_rb_big (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k SETTINGS auto_statistics_types = '';
INSERT INTO t_rb_small SELECT number, number FROM numbers(1000);
INSERT INTO t_rb_big SELECT number % 1000, number FROM numbers(100000);

SELECT '-- 1000 rows of 2 MB each exceed the default budget: shuffle';
SET param__internal_join_table_stat_hints = '{"t_rb_small": {"cardinality": 1000, "avg_row_bytes": 2000000}, "t_rb_big": {"cardinality": 100000, "avg_row_bytes": 16}}';
EXPLAIN SELECT count() FROM t_rb_big AS b JOIN t_rb_small AS s ON b.k = s.k;

SELECT '-- the same rows at 16 bytes fit: broadcast';
SET param__internal_join_table_stat_hints = '{"t_rb_small": {"cardinality": 1000, "avg_row_bytes": 16}, "t_rb_big": {"cardinality": 100000, "avg_row_bytes": 16}}';
EXPLAIN SELECT count() FROM t_rb_big AS b JOIN t_rb_small AS s ON b.k = s.k;

SELECT '-- a budget of 0 turns the byte check off: broadcast of the 2 MB rows';
SET param__internal_join_table_stat_hints = '{"t_rb_small": {"cardinality": 1000, "avg_row_bytes": 2000000}, "t_rb_big": {"cardinality": 100000, "avg_row_bytes": 16}}';
EXPLAIN SELECT count() FROM t_rb_big AS b JOIN t_rb_small AS s ON b.k = s.k SETTINGS distributed_plan_max_bytes_to_broadcast = 0;

SELECT '-- a filter the primary key cannot prune leaves the rows unknown; the bound of 1000 rows fits the row limit: broadcast';
SET param__internal_join_table_stat_hints = '{}';
EXPLAIN SELECT count() FROM t_rb_big AS b JOIN t_rb_small AS s ON b.k = s.k WHERE s.v % 7 = 1;

SELECT '-- the same bound above a row limit of 500: shuffle';
EXPLAIN SELECT count() FROM t_rb_big AS b JOIN t_rb_small AS s ON b.k = s.k WHERE s.v % 7 = 1 SETTINGS distributed_plan_max_rows_to_broadcast = 500;

SELECT '-- a table that fits the row limit stays one read task, unless its bytes exceed the budget';
EXPLAIN SELECT sum(v) FROM t_rb_small;
EXPLAIN SELECT sum(v) FROM t_rb_small SETTINGS distributed_plan_max_bytes_to_broadcast = 1000;

SELECT '-- results do not depend on the shape';
SELECT count() FROM t_rb_big AS b JOIN t_rb_small AS s ON b.k = s.k WHERE s.v % 7 = 1
SETTINGS distributed_plan_execute_locally = 1;
SELECT count() FROM t_rb_big AS b JOIN t_rb_small AS s ON b.k = s.k WHERE s.v % 7 = 1
SETTINGS distributed_plan_execute_locally = 1, distributed_plan_max_rows_to_broadcast = 500;

DROP TABLE t_rb_small;
DROP TABLE t_rb_big;
