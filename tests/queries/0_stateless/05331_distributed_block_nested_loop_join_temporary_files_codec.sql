-- Tags: distributed
-- A join whose `ON` condition yields no keys runs as a block nested loop join, which spills its build
-- side through `temporary_files_codec`. The session's authorization of a gated codec has to reach a
-- remote shard with the serialized plan, or the shard rejects the codec at its first spill.

SET serialize_query_plan = 1;
SET prefer_localhost_replica = 0;
SET enable_analyzer = 1;
SET allow_block_nested_loop_join = 1;
SET max_bytes_before_external_join = '100K';
SET max_bytes_ratio_before_external_join = 0;

CREATE TEMPORARY TABLE start_ts AS ( SELECT now() AS ts );

-- Without the opt-in, the shard must reject the experimental codec, exactly like a local spill would.
SELECT count() FROM remote('127.0.0.{1,2}', view(
    SELECT a.number FROM numbers(10) AS a LEFT JOIN numbers(300000) AS b ON (a.number * b.number) % 1000 = 1))
SETTINGS temporary_files_codec = 'ZXC'; -- { serverError BAD_ARGUMENTS }

-- With the opt-in, the shard spills with the codec chosen by the initiator.
SELECT count() FROM remote('127.0.0.{1,2}', view(
    SELECT a.number FROM numbers(10) AS a LEFT JOIN numbers(300000) AS b ON (a.number * b.number) % 1000 = 1))
SETTINGS log_comment = '05331_distributed_block_nested_loop_join_temporary_files_codec', enable_zxc_codec = 1, temporary_files_codec = 'ZXC';

SYSTEM FLUSH LOGS system.query_log;

-- The spill has to happen on the shard (`is_initial_query = 0`). The shard rows are attributed to this
-- test through the initiator query, see `04647_distributed_external_aggregation_temporary_files_codec`.
SELECT sum(ProfileEvents['ExternalJoinWritePart']) > 0
FROM system.query_log
WHERE event_date >= yesterday() AND event_time >= (SELECT ts FROM start_ts)
    AND type != 1
    AND is_initial_query = 0
    AND log_comment = '05331_distributed_block_nested_loop_join_temporary_files_codec'
    AND initial_query_id IN (
        SELECT query_id
        FROM system.query_log
        WHERE event_date >= yesterday() AND event_time >= (SELECT ts FROM start_ts)
            AND type != 1
            AND is_initial_query
            AND current_database = currentDatabase()
            AND log_comment = '05331_distributed_block_nested_loop_join_temporary_files_codec');
