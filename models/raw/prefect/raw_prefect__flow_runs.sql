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
    )

SELECT *
FROM flow_runs