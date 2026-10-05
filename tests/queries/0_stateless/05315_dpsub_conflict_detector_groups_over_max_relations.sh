#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# With a conflict detector, a join tree goes to DPsub as one group of tables with its semi/anti and
# outer joins. DPsub plans at most 12 tables, so a larger tree is cut into groups of at most 12, each
# planned by DPsub with the conflict detector; the semi/anti joins stay part of a group instead of
# being left to swap with their neighbour.
#
# A condition over three tables (`a.x + b.x = c.x`) ties them together like a two-table condition
# does, so DPsub plans such a query instead of turning it down as disconnected.
#
# For each query print the sizes of the groups the join order optimizer was given and the algorithms
# that planned them, and check the result against the query without reordering.

# Pinned because the harness randomizes them and they change how the groups are formed.
SETTINGS=(
    --query_plan_optimize_join_order_randomize 0
    --query_plan_convert_outer_join_to_inner_join 0
    --automatic_parallel_replicas_mode 0
    --join_use_nulls 0
    --query_plan_optimize_join_order_algorithm 'dpsub,greedy'
    --query_plan_optimize_join_order_conflict_detector 'c'
)

check()
{
    local limit="$1" query="$2"
    $CLICKHOUSE_CLIENT "${SETTINGS[@]}" --query_plan_optimize_join_order_limit "$limit" --send_logs_level trace -q "$query FORMAT Null" 2>&1 \
        | grep -oE 'query graph with [0-9]+ relations|Solving join order using [A-Z]+ algorithm' \
        | sed -E 's/query graph with ([0-9]+) relations/group of \1/; s/Solving join order using ([A-Z]+) algorithm/planned by \1/'
    local reordered expected
    reordered=$($CLICKHOUSE_CLIENT "${SETTINGS[@]}" --query_plan_optimize_join_order_limit "$limit" -q "$query")
    expected=$($CLICKHOUSE_CLIENT "${SETTINGS[@]}" --query_plan_optimize_join_order_limit 0 -q "$query")
    [ "$reordered" = "$expected" ] && echo "same result" || echo "DIFFERENT RESULT: $reordered vs $expected"
}

# Unique keys and a chain over different columns (`g1.v = g2.k`, `g2.v = g3.k`, ...): every join
# matches at most one row per row, so the query stays small, and no column is shared along the
# chain, so the tables do not all fall into one equivalence class (a clique DPsub plans slowly).
create=""
for i in $(seq 1 14); do
    create+="DROP TABLE IF EXISTS g${i}_05315;
    CREATE TABLE g${i}_05315 (k Int32, v Int32) ENGINE = MergeTree ORDER BY k;
    INSERT INTO g${i}_05315 SELECT number, number + 1 FROM numbers(200);"
done
$CLICKHOUSE_CLIENT -q "$create"

from="g1_05315"
for i in $(seq 2 12); do
    from+=" JOIN g${i}_05315 ON g$((i - 1))_05315.v = g${i}_05315.k"
done
from+=" LEFT SEMI JOIN g13_05315 ON g1_05315.k = g13_05315.k LEFT ANTI JOIN g14_05315 ON g1_05315.v = g14_05315.v + 1000"

echo "-- 14 tables with a semi and an anti join, join order limit 20"
check 20 "SELECT count(), sum(g1_05315.v) FROM $from"

$CLICKHOUSE_CLIENT -q "
    DROP TABLE IF EXISTS a_05315;
    DROP TABLE IF EXISTS b_05315;
    DROP TABLE IF EXISTS c_05315;
    CREATE TABLE a_05315 (x Int32) ENGINE = MergeTree ORDER BY x;
    CREATE TABLE b_05315 (x Int32) ENGINE = MergeTree ORDER BY x;
    CREATE TABLE c_05315 (x Int32) ENGINE = MergeTree ORDER BY x;
    INSERT INTO a_05315 SELECT number FROM numbers(30);
    INSERT INTO b_05315 SELECT number FROM numbers(20);
    INSERT INTO c_05315 SELECT number FROM numbers(40);
"

echo "-- a condition over three tables"
check 10 "SELECT count() FROM a_05315 CROSS JOIN b_05315 JOIN c_05315 ON a_05315.x + b_05315.x = c_05315.x"

drop="DROP TABLE a_05315; DROP TABLE b_05315; DROP TABLE c_05315;"
for i in $(seq 1 14); do
    drop+=" DROP TABLE g${i}_05315;"
done
$CLICKHOUSE_CLIENT -q "$drop"
