{{ config(
    alias="paciente_eventos_recentes",
    materialized='table',
    partition_by={"field": "datalake__imported_at", "data_type": "timestamp"},
    partition_expiration_days=7,
    clustering_by=["gid"],
    tags=["vitai_db"]
) }}

with recent_events as (
    select
        *
    from
        {{ ref("raw_prontuario_vitai__paciente_eventos") }}
    where
        datalake__imported_at >= timestamp_sub(current_timestamp(), interval 7 day)
)

select * from recent_events