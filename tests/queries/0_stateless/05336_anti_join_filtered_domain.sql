-- `NOT EXISTS` over a filtered table with several rows per key. The filter kept the key's NDV
-- (only the rows went down), the join below the anti join narrowed the key values of its two sides
-- to the smaller count, and the anti join then saw every preserved key matched and estimated one
-- row. Now a filter leaves a value only when one of its rows survives, two filtered sets of one
-- key domain overlap in proportion to their shares of it, and a semi or anti join counts the
-- preserved keys the other side has. The buffered subquery result also has an estimate.
--
-- `t_ae_fact` has four rows per key with a pseudo-random `v`; `v < 334` keeps a third of them, so
-- a key keeps at least one row with probability 1 - (2/3)^4 = 0.8 and the anti join keeps about a
-- fifth of the preserved rows.
SET allow_experimental_statistics = 1;
SET use_statistics = 1;
-- The test runner may turn this off; the inserted parts need their statistics.
SET materialize_statistics_on_insert = 1;
SET explain_query_plan_default = 'legacy';
SET enable_analyzer = 1;
SET enable_parallel_replicas = 0;
SET allow_correlated_subqueries = 1;
SET query_plan_optimize_join_order_limit = 10;
SET query_plan_optimize_join_order_randomize = 0;
SET query_plan_optimize_join_order_algorithm = 'greedy';
SET query_plan_join_swap_table = 0;
SET enable_join_runtime_filters = 0;
SET collect_hash_table_stats_during_joins = 0;
SET max_rows_to_group_by = 0;
SET max_rows_in_join = 0;
SET max_bytes_in_join = 0;
SET param__internal_cascades_cluster_node_count = 3;

DROP TABLE IF EXISTS t_ae_fact;
DROP TABLE IF EXISTS t_ae_keys;

CREATE TABLE t_ae_fact (k UInt64 STATISTICS(uniq, basic), v UInt64 STATISTICS(uniq, basic))
    ENGINE = MergeTree ORDER BY k SETTINGS auto_statistics_types = '';
CREATE TABLE t_ae_keys (k UInt64 STATISTICS(uniq, basic))
    ENGINE = MergeTree ORDER BY k SETTINGS auto_statistics_types = '';
INSERT INTO t_ae_fact SELECT number % 10000, cityHash64(number) % 1000 FROM numbers(40000);
INSERT INTO t_ae_keys SELECT number FROM numbers(10000);

SELECT '-- rows of the anti join';
SELECT count() FROM t_ae_keys AS a WHERE NOT EXISTS (SELECT 1 FROM t_ae_fact AS f WHERE f.k = a.k AND f.v < 334);

SELECT '-- estimate, buffered subquery';
EXPLAIN estimates = 1
SELECT count() FROM t_ae_keys AS a WHERE NOT EXISTS (SELECT 1 FROM t_ae_fact AS f WHERE f.k = a.k AND f.v < 334)
SETTINGS correlated_subqueries_use_in_memory_buffer = 1;

SELECT '-- estimate, subquery repeated';
EXPLAIN estimates = 1
SELECT count() FROM t_ae_keys AS a WHERE NOT EXISTS (SELECT 1 FROM t_ae_fact AS f WHERE f.k = a.k AND f.v < 334)
SETTINGS correlated_subqueries_use_in_memory_buffer = 0;

SELECT '-- Cascades';
EXPLAIN estimates = 1
SELECT count() FROM t_ae_keys AS a WHERE NOT EXISTS (SELECT 1 FROM t_ae_fact AS f WHERE f.k = a.k AND f.v < 334)
SETTINGS make_distributed_plan = 1, enable_cascades_optimizer = 1, correlated_subqueries_use_in_memory_buffer = 0;

DROP TABLE t_ae_fact;
DROP TABLE t_ae_keys;
