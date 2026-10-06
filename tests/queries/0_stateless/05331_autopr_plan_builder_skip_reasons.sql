-- The builder of the plan with parallel replicas checks what it can before building anything. When it
-- stops there, the query is counted as skipped - by its settings or by its shape - not as a plan that
-- was built (`AutoParallelReplicasPlanBuildAttempts`) and turned out unsuitable
-- (`AutoParallelReplicasPlanNotSuitable`).

DROP TABLE IF EXISTS t_autopr_builder_skip;

CREATE TABLE t_autopr_builder_skip (a UInt64, b UInt64) ENGINE = MergeTree ORDER BY a;
INSERT INTO t_autopr_builder_skip SELECT number, number % 100 FROM numbers(100000);

SET enable_analyzer = 1, enable_parallel_replicas = 1, automatic_parallel_replicas_mode = 1,
    parallel_replicas_local_plan = 1, parallel_replicas_for_non_replicated_merge_tree = 1,
    max_parallel_replicas = 3, automatic_parallel_replicas_min_bytes_per_replica = 0,
    cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost';

-- Settings: without a local plan there is nothing to compare.
SELECT b, count() FROM t_autopr_builder_skip GROUP BY b FORMAT Null
SETTINGS parallel_replicas_local_plan = 0, log_comment = 'autopr_builder_skip_settings';

-- Shape: parallel replicas cannot read a non-replicated MergeTree when that is not allowed.
SELECT b, count() FROM t_autopr_builder_skip GROUP BY b FORMAT Null
SETTINGS parallel_replicas_for_non_replicated_merge_tree = 0, log_comment = 'autopr_builder_skip_shape';

-- Built: the plan is built, and its outcome is counted separately.
SELECT b, count() FROM t_autopr_builder_skip GROUP BY b FORMAT Null
SETTINGS log_comment = 'autopr_builder_skip_built';

SET enable_parallel_replicas = 0, automatic_parallel_replicas_mode = 0;

SYSTEM FLUSH LOGS query_log;

SELECT log_comment,
       ProfileEvents['AutoParallelReplicasSkippedDueToSettings'] AS skipped_due_to_settings,
       ProfileEvents['AutoParallelReplicasPlanShapeNotSupported'] AS shape_not_supported,
       ProfileEvents['AutoParallelReplicasPlanBuildAttempts'] AS built,
       ProfileEvents['AutoParallelReplicasPlanNotSuitable'] AS not_suitable
FROM system.query_log
WHERE (event_date >= yesterday()) AND (event_time >= (NOW() - toIntervalMinute(15)))
    AND (current_database = currentDatabase())
    AND startsWith(log_comment, 'autopr_builder_skip_')
    AND (type = 'QueryFinish') AND is_initial_query
ORDER BY log_comment;

DROP TABLE t_autopr_builder_skip;
