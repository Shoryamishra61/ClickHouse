-- an indexHint derived from QUALIFY must stay above the window, otherwise it prunes rows of surviving partitions

SET optimize_and_compare_chain = 1;

DROP TABLE IF EXISTS t_hint_window;
CREATE TABLE t_hint_window (k UInt32, v UInt32, w UInt32) ENGINE = MergeTree ORDER BY v;
INSERT INTO t_hint_window SELECT number % 10, 50 + number, 60 + number FROM numbers(100);
INSERT INTO t_hint_window SELECT number % 10, number, number + 1 FROM numbers(30);
SELECT sum(s) FROM (SELECT k, v, w, sum(v) OVER (PARTITION BY k) AS s FROM t_hint_window QUALIFY v <= w AND w < 41);
DROP TABLE t_hint_window;

DROP TABLE IF EXISTS t_hint_window_dt64;
CREATE TABLE t_hint_window_dt64 (k UInt32, v DateTime64(3, 'UTC'), w DateTime64(3, 'UTC')) ENGINE = MergeTree ORDER BY v;
INSERT INTO t_hint_window_dt64 SELECT number % 10, toDateTime64(50 + number, 3, 'UTC'), toDateTime64(60 + number, 3, 'UTC') FROM numbers(100);
INSERT INTO t_hint_window_dt64 SELECT number % 10, toDateTime64(number, 3, 'UTC'), toDateTime64(number + 1, 3, 'UTC') FROM numbers(30);
SELECT sum(s) FROM (SELECT k, v, w, count() OVER (PARTITION BY k) AS s FROM t_hint_window_dt64 QUALIFY v <= w AND w < toDateTime(41, 'UTC'));
DROP TABLE t_hint_window_dt64;
