{{
    config(
        materialized="table",
        schema="brutos_prefect",
        alias="flow_runs"
    )
}}

WITH
    flow_runs AS (
        SELECT * 
        FROM {{ source("brutos_prefect_staging", "flow_state_change") }}
    ),

    flow_runs_grouped AS (
        SELECT
            flow_name,
            flow_id,
            flow_run_id,
            ANY_VALUE(flow_parameters) AS flow_parameters,
            ARRAY_AGG(STRUCT(state, message, occurrence)) AS states,
            MAX(occurrence) AS last_updated
        FROM flow_runs
        GROUP BY flow_name, flow_id, flow_run_id
    )

SELECT *
FROM flow_runs_grouped
ORDER BY last_updated DESC