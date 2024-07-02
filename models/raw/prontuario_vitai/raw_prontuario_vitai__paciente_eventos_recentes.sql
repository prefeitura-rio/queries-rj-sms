{{ config(
    materialized='table',
    partition_by={"field": "updated_at", "data_type": "timestamp"},
    clustering_by=["cpf"]
) }}

with recent_events as (
    select
        *
    from
        {{ ref("raw_prontuario_vitai__paciente_eventos") }}
    where
        datalake_imported_at >= timestamp_sub(current_timestamp(), interval 7 day)
)

select * from recent_events