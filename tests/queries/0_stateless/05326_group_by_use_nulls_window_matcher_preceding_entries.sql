-- The `REPLACE` transformer of a matcher in a window definition rewrites only the entries following it:
-- the preceding `PARTITION BY` / `ORDER BY` entries are already resolved and stay bound to the source columns.
-- Under `group_by_use_nulls` the matchers are expanded in advance, and the preceding entries must not be rewritten either.
-- Every query below is run at `group_by_use_nulls = 0` and at `group_by_use_nulls = 1`, and the two
-- results have to be the same, except for the NULL key of the total row.

SET enable_analyzer = 1;

SELECT '-- PARTITION BY of a named window';
SELECT t.c, count() OVER w FROM (SELECT number AS c FROM numbers(4)) AS t
GROUP BY t.c WITH ROLLUP WINDOW w AS (PARTITION BY c, * REPLACE (intDiv(c, 2) AS c))
ORDER BY t.c NULLS LAST, 2 SETTINGS group_by_use_nulls = 0;
SELECT t.c, count() OVER w FROM (SELECT number AS c FROM numbers(4)) AS t
GROUP BY t.c WITH ROLLUP WINDOW w AS (PARTITION BY c, * REPLACE (intDiv(c, 2) AS c))
ORDER BY t.c NULLS LAST, 2 SETTINGS group_by_use_nulls = 1;

SELECT '-- PARTITION BY of a window written in place';
SELECT t.c, count() OVER (PARTITION BY c, * REPLACE (intDiv(c, 2) AS c)) FROM (SELECT number AS c FROM numbers(4)) AS t
GROUP BY t.c WITH ROLLUP ORDER BY t.c NULLS LAST, 2 SETTINGS group_by_use_nulls = 0;
SELECT t.c, count() OVER (PARTITION BY c, * REPLACE (intDiv(c, 2) AS c)) FROM (SELECT number AS c FROM numbers(4)) AS t
GROUP BY t.c WITH ROLLUP ORDER BY t.c NULLS LAST, 2 SETTINGS group_by_use_nulls = 1;

SELECT '-- ORDER BY of a named window';
SELECT t.c, count() OVER w FROM (SELECT number AS c FROM numbers(4)) AS t
GROUP BY t.c WITH ROLLUP WINDOW w AS (ORDER BY c DESC, * REPLACE (intDiv(c, 2) AS c) ROWS UNBOUNDED PRECEDING)
ORDER BY t.c NULLS LAST, 2 SETTINGS group_by_use_nulls = 0;
SELECT t.c, count() OVER w FROM (SELECT number AS c FROM numbers(4)) AS t
GROUP BY t.c WITH ROLLUP WINDOW w AS (ORDER BY c DESC, * REPLACE (intDiv(c, 2) AS c) ROWS UNBOUNDED PRECEDING)
ORDER BY t.c NULLS LAST, 2 SETTINGS group_by_use_nulls = 1;

SELECT '-- ORDER BY of a window written in place';
SELECT t.c, count() OVER (ORDER BY c DESC, * REPLACE (intDiv(c, 2) AS c) ROWS UNBOUNDED PRECEDING) FROM (SELECT number AS c FROM numbers(4)) AS t
GROUP BY t.c WITH ROLLUP ORDER BY t.c NULLS LAST, 2 SETTINGS group_by_use_nulls = 0;
SELECT t.c, count() OVER (ORDER BY c DESC, * REPLACE (intDiv(c, 2) AS c) ROWS UNBOUNDED PRECEDING) FROM (SELECT number AS c FROM numbers(4)) AS t
GROUP BY t.c WITH ROLLUP ORDER BY t.c NULLS LAST, 2 SETTINGS group_by_use_nulls = 1;
