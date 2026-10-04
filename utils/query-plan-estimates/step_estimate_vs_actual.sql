-- Estimated against actual rows per query plan step, from system.processors_profile_log.
-- Requires `log_processors_profiles = 1` for the measured query. Pass the query id as the
-- {query_id:String} parameter, for example:
--   clickhouse-client --param_query_id='...' --queries-file step_estimate_vs_actual.sql
--
-- Every processor carries the row estimate of the plan step that created it. A step's actual
-- output is the sum of `output_rows` over its processors whose consumers belong to other steps;
-- `parent_ids` lists the consumers of a processor, so a chain inside a step is not counted twice.
-- A processor that feeds both its own step and another step is counted with all its outputs;
-- steps built that way need per-port accounting and are not comparable here.
-- q-error is max(estimate / actual, actual / estimate); it is NULL when either side is zero.
WITH
    log AS
    (
        SELECT id, parent_ids, step_uniq_id, plan_step_name, plan_step_description,
               plan_step_estimated_rows, plan_step_estimate_source, output_rows
        FROM system.processors_profile_log
        WHERE query_id = {query_id:String}
    ),
    internal AS
    (
        SELECT l.id AS id
        FROM log AS l
        ARRAY JOIN l.parent_ids AS consumer_id
        INNER JOIN log AS c ON c.id = consumer_id
        WHERE c.step_uniq_id = l.step_uniq_id
        GROUP BY l.id
    ),
    steps AS
    (
        SELECT step_uniq_id, any(plan_step_name) AS step, any(plan_step_description) AS description,
               any(plan_step_estimated_rows) AS estimated_rows, any(plan_step_estimate_source) AS source,
               sum(output_rows) AS actual_rows
        FROM log
        WHERE id NOT IN (SELECT id FROM internal)
        GROUP BY step_uniq_id
    )
SELECT step, description, estimated_rows, source, actual_rows,
       if(estimated_rows > 0 AND actual_rows > 0,
          greatest(estimated_rows / actual_rows, actual_rows / estimated_rows), NULL) AS q_error
FROM steps
ORDER BY q_error DESC NULLS LAST, step;
