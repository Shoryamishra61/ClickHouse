-- The join order optimizer stamps the row estimate of every relation and every join it decides
-- on the plan node, and `EXPLAIN estimates = 1` prints it with the origin of the estimate. The
-- hints stand in for column statistics so the numbers are fixed.
SET explain_query_plan_default = 'legacy';
SET enable_analyzer = 1;
SET enable_parallel_replicas = 0;
SET query_plan_optimize_join_order_limit = 10;
SET query_plan_optimize_join_order_randomize = 0;
SET query_plan_join_swap_table = 0;
SET use_statistics = 0;
SET param__internal_join_table_stat_hints = '{"t_est_a": {"cardinality": 1000, "distinct_keys": {"k": 1000}}, "t_est_b": {"cardinality": 100, "distinct_keys": {"k": 100}}}';

DROP TABLE IF EXISTS t_est_a;
DROP TABLE IF EXISTS t_est_b;

CREATE TABLE t_est_a (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k SETTINGS auto_statistics_types = '';
CREATE TABLE t_est_b (k UInt64, w UInt64) ENGINE = MergeTree ORDER BY k SETTINGS auto_statistics_types = '';
INSERT INTO t_est_a SELECT number, number FROM numbers(1000);
INSERT INTO t_est_b SELECT number * 10, number FROM numbers(100);

EXPLAIN estimates = 1
SELECT count()
FROM t_est_a AS a
JOIN t_est_b AS b ON a.k = b.k;

-- Without hints and statistics the relations are estimated from the primary index.
SET param__internal_join_table_stat_hints = '{}';
EXPLAIN estimates = 1
SELECT count()
FROM t_est_a AS a
JOIN t_est_b AS b ON a.k = b.k
WHERE b.w < 10;

DROP TABLE t_est_a;
DROP TABLE t_est_b;
