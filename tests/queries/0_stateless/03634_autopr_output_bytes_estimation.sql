-- Tags: stateful, long, no-msan

SET enable_parallel_replicas=1, automatic_parallel_replicas_mode=2, parallel_replicas_local_plan=1, parallel_replicas_index_analysis_only_on_coordinator=1,
    parallel_replicas_for_non_replicated_merge_tree=1, max_parallel_replicas=3, cluster_for_parallel_replicas='parallel_replicas';
SET optimize_move_to_prewhere = 1, query_plan_optimize_lazy_materialization = 1, query_plan_max_limit_for_lazy_materialization = 10000;

-- External aggregation is not supported as of now
SET max_bytes_before_external_group_by=0, max_bytes_ratio_before_external_group_by=0;

-- Disable external sorting. When the randomized `max_bytes_before_external_sort` makes `query_43`
-- spill, and the randomized `prefer_external_sort_block_bytes` is as low as 1, the merged sorted
-- output comes out in blocks of 128 rows. The estimator samples those small blocks, which compress
-- several times worse than full-size ones, so the `URL` output estimate grows from ~17 MB to ~60 MB,
-- beyond the tolerance below.
SET max_bytes_before_external_sort=0, max_bytes_ratio_before_external_sort=0;

-- Override randomized max_threads to avoid timeout on slow builds (ASan)
SET max_threads=0;

-- Override randomized max_block_size so the output-bytes estimate stays deterministic.
-- `RuntimeDataflowStatisticsOutputBytes` is accumulated per block (the output columns are
-- serialized block-by-block with the default codec), so a randomized `max_block_size` shifts
-- the estimate and can push `query_43`'s `URL` output past the tolerance below. The expected
-- sizes are calibrated for the default `max_block_size` (65409).
SET max_block_size=65409;

-- The aggregation-state size estimate is recorded per bucket after the conversion to
-- a two-level hash table, so forcing the conversion from the very first block (the test
-- randomization sets these thresholds as low as 1) shifts the estimate well away from the
-- expected values calibrated under the default thresholds. Pin them to the defaults.
SET group_by_two_level_threshold=100000, group_by_two_level_threshold_bytes=50000000;

-- For the same reason, disable the adaptive aggregator. Its per-thread tables stay single-level
-- until one of them reaches `adaptive_aggregator_freeze_threshold` keys and freezes, which converts
-- the data to two-level. Whether a thread crosses the threshold depends on how the marks happen to be
-- distributed between the reading threads, so `query_12` randomly took the two-level bucket merge,
-- whose estimate (~6.6M) is 2.5x the single-level one the expected values are calibrated for.
SET enable_adaptive_aggregator=0;

SELECT COUNT(*) FROM test.hits WHERE AdvEngineID <> 0 FORMAT Null SETTINGS log_comment='query_1';

-- Unsupported at the moment, refer to comments in `RuntimeDataflowStatisticsCacheUpdater::recordAggregationStateSizes`
-- SELECT COUNT(DISTINCT SearchPhrase) FROM test.hits FORMAT Null SETTINGS log_comment='query_5';

SELECT MobilePhoneModel, COUNT(DISTINCT UserID) AS u FROM test.hits WHERE MobilePhoneModel <> '' GROUP BY MobilePhoneModel ORDER BY u DESC LIMIT 10 FORMAT Null SETTINGS log_comment='query_10';

SELECT SearchPhrase, COUNT(*) AS c FROM test.hits WHERE SearchPhrase <> '' GROUP BY SearchPhrase ORDER BY c DESC LIMIT 10 FORMAT Null SETTINGS log_comment='query_12';

SELECT UserID, COUNT(*) FROM test.hits GROUP BY UserID ORDER BY COUNT(*) DESC LIMIT 10 FORMAT Null SETTINGS log_comment='query_15';

SELECT COUNT(*) FROM test.hits WHERE URL LIKE '%google%' FORMAT Null SETTINGS log_comment='query_20';

SELECT SearchPhrase, MIN(URL), COUNT(*) AS c FROM test.hits WHERE URL LIKE '%google%' AND SearchPhrase <> '' GROUP BY SearchPhrase ORDER BY c DESC LIMIT 10 FORMAT Null SETTINGS log_comment='query_21';

SELECT SearchPhrase, MIN(URL), MIN(Title), COUNT(*) AS c, COUNT(DISTINCT UserID) FROM test.hits WHERE Title LIKE '%Google%' AND URL NOT LIKE '%.google.%' AND SearchPhrase <> '' GROUP BY SearchPhrase ORDER BY c DESC LIMIT 10 FORMAT Null SETTINGS log_comment='query_22';

SELECT * FROM test.hits WHERE URL LIKE '%google%' ORDER BY EventTime LIMIT 10 FORMAT Null SETTINGS log_comment='query_23';

SELECT REGEXP_REPLACE(Referer, '^https?://(?:www\.)?([^/]+)/.*$', '\1') AS k, AVG(length(Referer)) AS l, COUNT(*) AS c, MIN(Referer) FROM test.hits WHERE Referer <> '' GROUP BY k HAVING COUNT(*) > 100000 ORDER BY l DESC LIMIT 25 FORMAT Null SETTINGS log_comment='query_28';

SELECT 1, URL, COUNT(*) AS c FROM test.hits GROUP BY 1, URL ORDER BY c DESC LIMIT 10 FORMAT Null SETTINGS log_comment='query_34';

SELECT URL from test.hits WHERE URL LIKE '%yandex%' ORDER BY URL DESC FORMAT Null SETTINGS log_comment='query_43';

-- Unsupported case: filtering by set built from subquery
--SELECT * FROM test.hits WHERE CounterID IN (SELECT CounterID % 1000 FROM test.hits) FORMAT Null SETTINGS log_comment='query_44';

SET enable_parallel_replicas=0, automatic_parallel_replicas_mode=0;

SYSTEM FLUSH LOGS query_log;

-- Check the output estimate against what the replicas actually send (ratio within 2.5x).
-- The expected values are `NetworkReceiveBytes` on the initiator, measured per query with
-- `parallel_replicas_local_plan = 0`, `prefer_localhost_replica = 0` and compression forced on every
-- replica of the cluster. Forcing it is what makes the measurement meaningful: every replica address of
-- this cluster looks local and is therefore shipped uncompressed by default (see `Cluster.cpp`), which
-- is several times more bytes than any real cluster transfers. `serialize_query_plan` moves the estimate
-- by less than 0.4%, so one set of values covers both the query-based and the plan-based implementation.
--
-- Queries whose transfer stays under 100 KB are not checked: at that size the bytes on the wire are
-- mostly protocol framing and per-replica fixed cost, which the estimate does not model and should not.
-- Their measured transfers are recorded all the same, so the pairs can be read off this array.
--
-- The checked queries land within 1.07x to 1.80x of the transferred bytes: `query_28` 1.07x, `query_15`
-- 1.16x, `query_43` 1.18x, `query_12` 1.20x, `query_34` 1.63x and `query_10` 1.80x - the last two under
-- rather than over. Aggregate states are the residual: they are sampled from the hash table and so priced
-- in hash-table order, while the replicas send them in key order.
WITH
    [17258, 362202, 5372342, 1992487, 17111, 33142, 73083, 58884, 38520304, 130028333, 32692272/*, 641835*/] AS expected_bytes,
    arrayJoin(arrayMap(x -> (untuple(x.1), x.2), arrayZip(res, expected_bytes))) AS res
SELECT format('{} {} {}', res.1, res.2, res.3)
FROM
(
    SELECT groupArray((log_comment, output_bytes)) AS res
    FROM (
      SELECT log_comment, ProfileEvents['RuntimeDataflowStatisticsOutputBytes'] output_bytes
      FROM system.query_log
      WHERE (event_date >= yesterday()) AND (event_time >= (NOW() - toIntervalMinute(15))) AND (current_database = currentDatabase()) AND (log_comment LIKE 'query_%') AND (type = 'QueryFinish')
      ORDER BY event_time_microseconds
    )
)
WHERE res.3 >= 100000 AND (greatest(res.2, res.3) / least(res.2, res.3)) > 2.5;
