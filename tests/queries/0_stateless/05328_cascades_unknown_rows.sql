-- The Cascades optimizer gave a read it could not estimate zero rows, and a read without index
-- analysis a million rows, and reported both as estimates. Now such a read is marked unknown and
-- costed by the rows it cannot exceed, everything derived from it stays unknown, and the join it
-- feeds takes the join order optimizer's estimate, unknown included. `t_unk` has no statistics and
-- a filter the primary index cannot use; the hint stands in for the statistics of `t_known`.
SET explain_query_plan_default = 'legacy';
SET enable_analyzer = 1;
SET enable_parallel_replicas = 0;
SET enable_join_runtime_filters = 0;
SET collect_hash_table_stats_during_joins = 0;
SET query_plan_optimize_join_order_limit = 10;
SET query_plan_optimize_join_order_randomize = 0;
SET query_plan_optimize_join_order_algorithm = 'greedy';
SET query_plan_join_swap_table = 0;
SET use_statistics = 0;
SET param__internal_cascades_cluster_node_count = 3;
SET param__internal_join_table_stat_hints = '{"t_known": {"cardinality": 1000, "distinct_keys": {"k": 1000}}}';
SET max_rows_to_group_by = 0;

DROP TABLE IF EXISTS t_known;
DROP TABLE IF EXISTS t_unk;

CREATE TABLE t_known (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k SETTINGS auto_statistics_types = '';
CREATE TABLE t_unk (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k SETTINGS auto_statistics_types = '';
INSERT INTO t_known SELECT number, number FROM numbers(1000);
INSERT INTO t_unk SELECT number * 10, number FROM numbers(100);

-- The filtered read and the join above it are unknown; the aggregation of an unknown input is unknown.
EXPLAIN estimates = 1
SELECT k, count()
FROM t_known AS a
JOIN t_unk AS u ON a.k = u.k
WHERE u.v < 5
GROUP BY k
SETTINGS make_distributed_plan = 1, enable_cascades_optimizer = 1;

-- Without the filter the read is estimated from the primary index, and everything is known.
EXPLAIN estimates = 1
SELECT k, count()
FROM t_known AS a
JOIN t_unk AS u ON a.k = u.k
GROUP BY k
SETTINGS make_distributed_plan = 1, enable_cascades_optimizer = 1;

DROP TABLE t_known;
DROP TABLE t_unk;
