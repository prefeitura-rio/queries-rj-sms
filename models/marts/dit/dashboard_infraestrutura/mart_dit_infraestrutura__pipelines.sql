{{
    config(
        materialized="table",
        alias="pipelines",
        partition_by={
            "field": "starting_time",
            "data_type": "date",
            "granularity": "month",
        },
    )
}}

WITH
    flow_runs AS (
        SELECT * FROM {{ ref("raw_prefect__flow_runs") }}
    ),

    -- Discovering the running states
    all_states AS (
        SELECT
            flow_run_id,
            array_agg(
                struct(
                    state as state,
                    occurrence as occurrence
                )
            ) as states
        FROM flow_runs
        GROUP BY 1
    ),

    -- Discovering the ending states
    ending_states AS (
        SELECT
            flow_run_id,
            state as ending_state,
            occurrence as ending_time
        FROM flow_runs
        WHERE state in ('Success', 'Failed', 'Cancelled')
    ),

    -- Discovering the starting states
    starting_states AS (
        SELECT
            flow_run_id,
            state as starting_state,
            occurrence as starting_time
        FROM flow_runs
        WHERE flow_runs.state = 'Running'
        qualify row_number() over (partition by flow_run_id order by occurrence) = 1
    ),

    flow_runs_with_ending_states AS (
        SELECT
            flow_name,
            flow_id,
            flow_run_id,

            starting_states.starting_state,
            starting_states.starting_time,

            ending_states.ending_state,
            ending_states.ending_time,

            all_states.states,

            CASE
                -- It is possible to calculate the duration of the flow run
                WHEN ending_states.ending_time IS NOT NULL THEN
                    TIMESTAMP_DIFF(ending_states.ending_time, starting_states.starting_time, MINUTE)

                -- Else, calculate the duration of the flow run till now
                ELSE
                    TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), starting_states.starting_time, MINUTE)
            END as duration_minutes
        FROM flow_runs
            LEFT JOIN starting_states using (flow_run_id)
            LEFT JOIN ending_states using (flow_run_id)
            LEFT JOIN all_states using (flow_run_id)
    ),
    final as (
        select 
            * except(ending_state, ending_time, duration_minutes),
            CASE
                WHEN (ending_state is null) AND (duration_minutes > 60*24) THEN 'Crashed'
                ELSE ending_state
            END as ending_state,
            CASE 
                WHEN (ending_state is null) AND (duration_minutes > 60*24) THEN null
                ELSE ending_time
            END as ending_time,
            CASE 
                WHEN (ending_state is null) AND (duration_minutes > 60*24) THEN null
                ELSE duration_minutes
            END as duration_minutes
        from flow_runs_with_ending_states
    )

SELECT *
FROM final