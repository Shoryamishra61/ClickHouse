-- A join on several keys takes the selectivity of the most selective key alone, in the join order
-- optimizer and in the Cascades optimizer alike. `query_plan_join_selectivity_exponential_backoff`
-- combines the keys instead: sorted from the most selective, they count with the exponents 1, 1/2,
-- 1/4 and 1/8. Two predicates that relate the same pair of equivalent columns count once, and a
-- key without an NDV counts its relation's rows as its NDV, as if it were unique.
-- The hints stand in for column statistics; `t_unk_a` and `t_unk_b` have none.
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
SET param__internal_join_table_stat_hints = '{"t_keys_l": {"cardinality": 1000000, "distinct_keys": {"k1": 1000, "k2": 100}}, "t_keys_r": {"cardinality": 10000, "distinct_keys": {"k1": 1000, "k2": 100}}, "t_keys_x": {"cardinality": 100000, "distinct_keys": {"k1": 1000}}}';

DROP TABLE IF EXISTS t_keys_l;
DROP TABLE IF EXISTS t_keys_r;
DROP TABLE IF EXISTS t_keys_x;
DROP TABLE IF EXISTS t_unk_a;
DROP TABLE IF EXISTS t_unk_b;

CREATE TABLE t_keys_l (k1 UInt64, k2 UInt64) ENGINE = MergeTree ORDER BY k1 SETTINGS auto_statistics_types = '';
CREATE TABLE t_keys_r (k1 UInt64, k2 UInt64) ENGINE = MergeTree ORDER BY k1 SETTINGS auto_statistics_types = '';
CREATE TABLE t_keys_x (k1 UInt64) ENGINE = MergeTree ORDER BY k1 SETTINGS auto_statistics_types = '';
CREATE TABLE t_unk_a (c UInt64) ENGINE = MergeTree ORDER BY c SETTINGS auto_statistics_types = '';
CREATE TABLE t_unk_b (c UInt64) ENGINE = MergeTree ORDER BY c SETTINGS auto_statistics_types = '';
INSERT INTO t_keys_l SELECT number % 1000, number % 100 FROM numbers(1000);
INSERT INTO t_keys_r SELECT number % 1000, number % 100 FROM numbers(1000);
INSERT INTO t_keys_x SELECT number % 1000 FROM numbers(1000);
INSERT INTO t_unk_a SELECT number FROM numbers(500);
INSERT INTO t_unk_b SELECT number FROM numbers(200);

-- 1000000 * 10000 / 1000 = 10000000 rows: the key with the larger NDV decides.
SELECT '-- most selective key';
EXPLAIN estimates = 1
SELECT count() FROM t_keys_l AS l JOIN t_keys_r AS r ON l.k1 = r.k1 AND l.k2 = r.k2;

-- 10000000 * sqrt(1 / 100) = 1000000 rows.
SELECT '-- exponential backoff';
EXPLAIN estimates = 1
SELECT count() FROM t_keys_l AS l JOIN t_keys_r AS r ON l.k1 = r.k1 AND l.k2 = r.k2
SETTINGS query_plan_join_selectivity_exponential_backoff = 1;

-- Without the join order optimizer the Cascades optimizer estimates the join itself, by the same rule.
SELECT '-- Cascades, exponential backoff';
EXPLAIN estimates = 1
SELECT count() FROM t_keys_l AS l JOIN t_keys_r AS r ON l.k1 = r.k1 AND l.k2 = r.k2
SETTINGS query_plan_join_selectivity_exponential_backoff = 1, query_plan_optimize_join_order_limit = 0,
    make_distributed_plan = 1, enable_cascades_optimizer = 1;

-- `l.k1 = x.k1` repeats `r.k1 = x.k1` (the first join makes `l.k1` and `r.k1` equal), so the second
-- join estimates the same rows with and without it.
SELECT '-- a predicate between equivalent columns counts once';
EXPLAIN estimates = 1
SELECT count() FROM t_keys_l AS l JOIN t_keys_r AS r ON l.k1 = r.k1 JOIN t_keys_x AS x ON r.k1 = x.k1
SETTINGS query_plan_join_selectivity_exponential_backoff = 1;
EXPLAIN estimates = 1
SELECT count() FROM t_keys_l AS l JOIN t_keys_r AS r ON l.k1 = r.k1 JOIN t_keys_x AS x ON r.k1 = x.k1 AND l.k1 = x.k1
SETTINGS query_plan_join_selectivity_exponential_backoff = 1;

-- `t_unk_a.c` has no NDV, so its 500 rows stand in; the larger NDV, the 1000 of `l.k1`, gives
-- 1000000 * 500 / 1000 = 500000 rows.
SELECT '-- a key with an NDV on one side';
EXPLAIN estimates = 1
SELECT count() FROM t_keys_l AS l JOIN t_unk_a AS a ON l.k1 = a.c;

-- No NDV on either side: the rows of each side stand in, 500 * 200 / 500 = 200 rows.
SELECT '-- a key without an NDV on either side';
EXPLAIN estimates = 1
SELECT count() FROM t_unk_a AS a JOIN t_unk_b AS b ON a.c = b.c;

DROP TABLE t_keys_l;
DROP TABLE t_keys_r;
DROP TABLE t_keys_x;
DROP TABLE t_unk_a;
DROP TABLE t_unk_b;
