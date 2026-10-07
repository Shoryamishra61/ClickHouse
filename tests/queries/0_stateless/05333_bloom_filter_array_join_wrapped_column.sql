-- Index analysis uses a `bloom_filter` index on an array for `arrayJoin` of the array inside `emptyArrayToSingle`
-- or inside a `Dynamic` cast round trip.

SET explain_query_plan_default = 'legacy';
SET use_skip_indexes = 1;
SET use_skip_indexes_on_data_read = 0;
SET use_query_condition_cache = 0;

DROP TABLE IF EXISTS t_bf_aj;

CREATE TABLE t_bf_aj
(
    n UInt64,
    tags Array(String),
    doc JSON(tags Array(String)),
    INDEX idx_n n TYPE bloom_filter GRANULARITY 1,
    INDEX idx_tags tags TYPE bloom_filter GRANULARITY 1,
    INDEX idx_doc_tags doc.tags TYPE bloom_filter GRANULARITY 1
)
ENGINE = MergeTree ORDER BY tuple()
SETTINGS index_granularity = 1, index_granularity_bytes = 0;

-- rows 8 and 9 have empty arrays
INSERT INTO t_bf_aj SELECT
    number,
    if(number < 8, ['t' || toString(number)], []),
    toJSONString(map('tags', if(number < 8, ['t' || toString(number)], [])))::JSON
FROM numbers(10);

SELECT '-- emptyArrayToSingle, value is not the default';
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM t_bf_aj WHERE arrayJoin(emptyArrayToSingle(tags)) = 't3') WHERE explain LIKE '%Name: idx%' OR explain LIKE '%Granules:%';
SELECT count() FROM t_bf_aj WHERE arrayJoin(emptyArrayToSingle(tags)) = 't3';
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM t_bf_aj WHERE arrayJoin(emptyArrayToSingle(tags)) IN ('t3', 't5')) WHERE explain LIKE '%Name: idx%' OR explain LIKE '%Granules:%';
SELECT count() FROM t_bf_aj WHERE arrayJoin(emptyArrayToSingle(tags)) IN ('t3', 't5');

SELECT '-- emptyArrayToSingle, value is the default';
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM t_bf_aj WHERE arrayJoin(emptyArrayToSingle(tags)) = '') WHERE explain LIKE '%Name: idx%' OR explain LIKE '%Granules:%';
SELECT count() FROM t_bf_aj WHERE arrayJoin(emptyArrayToSingle(tags)) = '';
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM t_bf_aj WHERE arrayJoin(emptyArrayToSingle(tags)) IN ('t3', '')) WHERE explain LIKE '%Name: idx%' OR explain LIKE '%Granules:%';
SELECT count() FROM t_bf_aj WHERE arrayJoin(emptyArrayToSingle(tags)) IN ('t3', '');

SELECT '-- Dynamic round trip';
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM t_bf_aj WHERE arrayJoin(tags::Dynamic::Array(String)) = 't3') WHERE explain LIKE '%Name: idx%' OR explain LIKE '%Granules:%';
SELECT count() FROM t_bf_aj WHERE arrayJoin(tags::Dynamic::Array(String)) = 't3';
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM t_bf_aj WHERE arrayJoin(tags::Dynamic::Array(Nullable(String))) = 't3') WHERE explain LIKE '%Name: idx%' OR explain LIKE '%Granules:%';
SELECT count() FROM t_bf_aj WHERE arrayJoin(tags::Dynamic::Array(Nullable(String))) = 't3';
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM t_bf_aj WHERE has(tags::Dynamic::Array(String), 't3')) WHERE explain LIKE '%Name: idx%' OR explain LIKE '%Granules:%';
SELECT count() FROM t_bf_aj WHERE has(tags::Dynamic::Array(String), 't3');

SELECT '-- Dynamic round trip that changes the value';
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM t_bf_aj WHERE n::Dynamic::String = '3') WHERE explain LIKE '%Name: idx%' OR explain LIKE '%Granules:%';
SELECT count() FROM t_bf_aj WHERE n::Dynamic::String = '3';

SELECT '-- both on a JSON path';
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM t_bf_aj WHERE arrayJoin(emptyArrayToSingle(doc.tags::Dynamic::Array(String))) GLOBAL IN (SELECT 't3')) WHERE explain LIKE '%Name: idx%' OR explain LIKE '%Granules:%';
SELECT count() FROM t_bf_aj WHERE arrayJoin(emptyArrayToSingle(doc.tags::Dynamic::Array(String))) GLOBAL IN (SELECT 't3');

SELECT '-- both on a JSON path, arrayJoin stays a function';
SET query_plan_lower_array_join_function = 0;
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM t_bf_aj WHERE arrayJoin(emptyArrayToSingle(doc.tags::Dynamic::Array(String))) GLOBAL IN (SELECT 't3')) WHERE explain LIKE '%Name: idx%' OR explain LIKE '%Granules:%';
SELECT count() FROM t_bf_aj WHERE arrayJoin(emptyArrayToSingle(doc.tags::Dynamic::Array(String))) GLOBAL IN (SELECT 't3');

DROP TABLE t_bf_aj;
