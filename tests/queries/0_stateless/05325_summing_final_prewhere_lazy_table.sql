-- Tags: no-replicated-database
-- A summed column consumed by `PREWHERE` must survive it to reach the `FINAL` merge, also when
-- the table is lazily loaded and the planner sees it through `StorageTableProxy`.

DROP DATABASE IF EXISTS {CLICKHOUSE_DATABASE_1:Identifier};
CREATE DATABASE {CLICKHOUSE_DATABASE_1:Identifier} ENGINE = Atomic SETTINGS lazy_load_tables = 1;

CREATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.t (a Int8, b Int32, k UInt64) ENGINE = SummingMergeTree ORDER BY k
    SETTINGS min_parts_to_merge_at_once = 10; -- `SYSTEM STOP MERGES` would not survive the reattach

-- `a` sums to zero for both keys, `b` only for key 2, so a real merge keeps key 1 only.
INSERT INTO {CLICKHOUSE_DATABASE_1:Identifier}.t VALUES (1, 5, 1), (1, 5, 2);
INSERT INTO {CLICKHOUSE_DATABASE_1:Identifier}.t VALUES (-1, 0, 1), (-1, -5, 2);

DETACH DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
ATTACH DATABASE {CLICKHOUSE_DATABASE_1:Identifier};

SELECT engine FROM system.tables WHERE database = currentDatabase() || '_1' AND name = 't';

SELECT count() FROM {CLICKHOUSE_DATABASE_1:Identifier}.t FINAL PREWHERE b IN (-5, 0, 5);
SELECT k FROM {CLICKHOUSE_DATABASE_1:Identifier}.t FINAL WHERE b IN (-5, 0, 5) ORDER BY k SETTINGS optimize_move_to_prewhere_if_final = 1;

-- The `FINAL` read must agree with the state after a real merge.
OPTIMIZE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.t FINAL;
SELECT k FROM {CLICKHOUSE_DATABASE_1:Identifier}.t ORDER BY k;

DROP DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
