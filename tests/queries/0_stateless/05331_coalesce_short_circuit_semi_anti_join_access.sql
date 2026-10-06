-- The analyzer folds `coalesce` and `ifNull` to the live argument when a statically unreachable
-- argument fails to resolve. Like the `if` and `multiIf` folding, it must not swallow
-- SEMI/ANTI JOIN access violations for columns of the non-preserved side.

SET enable_analyzer = 1;
SET semi_join_include_columns_from_both_sides = 0;
SET anti_join_include_columns_from_both_sides = 0;

SELECT coalesce(1, t2.b) FROM (SELECT 1 AS a) t1 LEFT SEMI JOIN (SELECT 2 AS b) t2 ON true; -- { serverError SEMI_ANTI_JOIN_COLUMN_ACCESS_DENIED }
SELECT coalesce(NULL, 1, t2.b) FROM (SELECT 1 AS a) t1 LEFT SEMI JOIN (SELECT 2 AS b) t2 ON true; -- { serverError SEMI_ANTI_JOIN_COLUMN_ACCESS_DENIED }
SELECT ifNull(1, t2.b) FROM (SELECT 1 AS a) t1 LEFT SEMI JOIN (SELECT 2 AS b) t2 ON true; -- { serverError SEMI_ANTI_JOIN_COLUMN_ACCESS_DENIED }
SELECT coalesce(1, t2.b) FROM (SELECT 1 AS a) t1 LEFT ANTI JOIN (SELECT 2 AS b) t2 ON false; -- { serverError SEMI_ANTI_JOIN_COLUMN_ACCESS_DENIED }
SELECT ifNull(1, t1.a) FROM (SELECT 1 AS a) t1 RIGHT SEMI JOIN (SELECT 2 AS b) t2 ON true; -- { serverError SEMI_ANTI_JOIN_COLUMN_ACCESS_DENIED }

-- Other analysis errors in an unreachable argument are still folded away.
SELECT coalesce(1, does_not_exist) FROM (SELECT 1 AS a) t1 LEFT SEMI JOIN (SELECT 2 AS b) t2 ON true;
SELECT ifNull(1, nonexistent_fn_xyz(1)) FROM (SELECT 1 AS a) t1 LEFT SEMI JOIN (SELECT 2 AS b) t2 ON true;
-- Columns of the preserved side are accessible.
SELECT coalesce(1, t1.a) FROM (SELECT 1 AS a) t1 LEFT SEMI JOIN (SELECT 2 AS b) t2 ON true;
