-- Tags: no-fasttest
-- Plan-based parallel replicas build an ordinary local plan, and the initiator's local replica executes a
-- clone of the fragment that is serialized for the remote replicas. A lambda body in that plan is
-- JIT-compiled for execution, while serialization ships its uncompiled body, so the local replica runs
-- the compiled lambda and the remote replicas compile it on their own.

DROP TABLE IF EXISTS t_05325;

CREATE TABLE t_05325 (id UInt64, arr Array(UInt64)) ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 128;
INSERT INTO t_05325 SELECT number, [number, number + 1] FROM numbers(100000);

SET enable_analyzer = 1;
SET enable_parallel_replicas = 1;
SET parallel_replicas_for_non_replicated_merge_tree = 1;
SET max_parallel_replicas = 3;
SET cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost';
SET parallel_replicas_plan_based = 1;
SET parallel_replicas_local_plan = 1;
SET automatic_parallel_replicas_mode = 0;
SET compile_expressions = 1, min_count_to_compile_expression = 0;

SELECT sum(arraySum(x -> (x * 3 + 7) * (x + 1) - x * 5 + 2, arr)) FROM t_05325 SETTINGS log_comment = '05325_compiled_lambda';

SET enable_parallel_replicas = 0;
SYSTEM FLUSH LOGS query_log;

-- The initiator's local replica executed the compiled lambda.
SELECT ProfileEvents['CompiledFunctionExecute'] > 0
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND is_initial_query AND log_comment = '05325_compiled_lambda';

DROP TABLE t_05325;
