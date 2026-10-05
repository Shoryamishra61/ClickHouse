-- Tags: no-fasttest
-- Index analysis matches a lambda by a name built from its body. A JIT-compiled lambda body used to be
-- named after the compiled expression, so a skip index over the same lambda expression was not used.

DROP TABLE IF EXISTS t_05326;

CREATE TABLE t_05326 (id UInt64, arr Array(UInt64), INDEX idx arrayExists(x -> x * 3 + 1 > 5000, arr) TYPE set(2) GRANULARITY 1)
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 128;
INSERT INTO t_05326 SELECT number, if(number < 1000, [number * 10], [1, 2]) FROM numbers(100000);

SET force_data_skipping_indices = 'idx';

SELECT count() FROM t_05326 WHERE arrayExists(x -> x * 3 + 1 > 5000, arr) SETTINGS compile_expressions = 0;
SELECT count() FROM t_05326 WHERE arrayExists(x -> x * 3 + 1 > 5000, arr) SETTINGS compile_expressions = 1, min_count_to_compile_expression = 0;

DROP TABLE t_05326;
