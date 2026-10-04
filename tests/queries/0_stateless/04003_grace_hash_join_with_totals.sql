-- `grace_hash` needs a spill threshold, which `max_bytes_ratio_before_external_join` no longer provides by default.
SET max_bytes_ratio_before_external_join = 0.5;

SET joined_subquery_requires_alias = 0;
SET join_algorithm = 'grace_hash';

SELECT k, s1, s2 FROM (SELECT intDiv(number, 3) AS k, sum(number) AS s1 FROM (SELECT * FROM system.numbers LIMIT 10) GROUP BY k) ANY LEFT JOIN (SELECT intDiv(number, 4) AS k, sum(number) AS s2 FROM (SELECT * FROM system.numbers LIMIT 10) GROUP BY k) USING k ORDER BY k;
SELECT k, s1, s2 FROM (SELECT intDiv(number, 3) AS k, sum(number) AS s1 FROM (SELECT * FROM system.numbers LIMIT 10) GROUP BY k WITH TOTALS) ANY LEFT JOIN (SELECT intDiv(number, 4) AS k, sum(number) AS s2 FROM (SELECT * FROM system.numbers LIMIT 10) GROUP BY k) USING k ORDER BY k;
SELECT k, s1, s2 FROM (SELECT intDiv(number, 3) AS k, sum(number) AS s1 FROM (SELECT * FROM system.numbers LIMIT 10) GROUP BY k) ANY LEFT JOIN (SELECT intDiv(number, 4) AS k, sum(number) AS s2 FROM (SELECT * FROM system.numbers LIMIT 10) GROUP BY k WITH TOTALS) USING k ORDER BY k;
SELECT k, s1, s2 FROM (SELECT intDiv(number, 3) AS k, sum(number) AS s1 FROM (SELECT * FROM system.numbers LIMIT 10) GROUP BY k WITH TOTALS) ANY LEFT JOIN (SELECT intDiv(number, 4) AS k, sum(number) AS s2 FROM (SELECT * FROM system.numbers LIMIT 10) GROUP BY k WITH TOTALS) USING k ORDER BY k;
