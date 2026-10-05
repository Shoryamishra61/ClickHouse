-- An aggregation over several keys estimates its groups as the largest key NDV, in the join order
-- optimizer, the distributed planner and the Cascades optimizer alike.
-- `query_plan_group_count_damped_product` combines the keys instead: sorted from the largest NDV,
-- they count with the exponents 1, 1/2, 1/4 and so on. The hints stand in for column statistics.
SET explain_query_plan_default = 'legacy';
SET enable_analyzer = 1;
SET enable_parallel_replicas = 0;
SET query_plan_optimize_join_order_limit = 10;
SET query_plan_optimize_join_order_randomize = 0;
SET query_plan_optimize_join_order_algorithm = 'greedy';
SET query_plan_join_swap_table = 0;
SET use_statistics = 0;
SET enable_join_runtime_filters = 0;
SET collect_hash_table_stats_during_joins = 0;
SET max_rows_to_group_by = 0;
SET param__internal_cascades_cluster_node_count = 3;
SET param__internal_join_table_stat_hints = '{"t_groups": {"cardinality": 1000000, "distinct_keys": {"k1": 100, "k2": 50}}, "t_dim": {"cardinality": 100, "distinct_keys": {"k": 100}}}';

DROP TABLE IF EXISTS t_groups;
DROP TABLE IF EXISTS t_dim;

CREATE TABLE t_groups (k1 UInt64, k2 UInt64, v UInt64) ENGINE = MergeTree ORDER BY k1 SETTINGS auto_statistics_types = '';
CREATE TABLE t_dim (k UInt64) ENGINE = MergeTree ORDER BY k SETTINGS auto_statistics_types = '';
INSERT INTO t_groups SELECT number % 100, number % 50, number FROM numbers(1000);
INSERT INTO t_dim SELECT number FROM numbers(100);

-- 100 groups: the larger NDV.
SELECT '-- largest key NDV';
EXPLAIN estimates = 1
SELECT count()
FROM (SELECT k1, k2, sum(v) AS s FROM t_groups GROUP BY k1, k2) AS g
JOIN t_dim AS d ON g.k1 = d.k;

-- 100 * sqrt(50) = 707 groups.
SELECT '-- damped product';
EXPLAIN estimates = 1
SELECT count()
FROM (SELECT k1, k2, sum(v) AS s FROM t_groups GROUP BY k1, k2) AS g
JOIN t_dim AS d ON g.k1 = d.k
SETTINGS query_plan_group_count_damped_product = 1;

SELECT '-- Cascades, damped product';
EXPLAIN estimates = 1
SELECT k1, k2, sum(v) FROM t_groups GROUP BY k1, k2
SETTINGS query_plan_group_count_damped_product = 1, make_distributed_plan = 1, enable_cascades_optimizer = 1;

DROP TABLE t_groups;
DROP TABLE t_dim;
