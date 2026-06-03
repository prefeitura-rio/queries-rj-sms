{{
    config(
        alias="profissional",
        materialized="incremental",
        schema="brutos_prontuario_mv",
        unique_key="id_hci",
        incremental_strategy="merge",
        partition_by={
            "field": "data_particao",
            "data_type": "date",
            "granularity": "day",
        },
        tags=["mv"],
    )
}}

with

    source as (
        select *
        from {{ source("brutos_prontuario_mv_api_staging", "profissional_continuo") }}
        {% if is_incremental() %}
            where
                timestamp_trunc(datalake_loaded_at, day) >= timestamp(
                    date_sub(current_date('America/Sao_Paulo'), interval 7 day)
                )
        {% endif %}
    ),

    profissional_json as (
        select
            payload_cnes as id_cnes,
            json_extract_scalar(data, '$.nome') as nome,
            json_extract_scalar(data, '$.cns') as cns,
            json_extract_scalar(data, '$.cpf') as cpf,
            json_extract_scalar(data, '$.cbo') as cbo,
            datalake_loaded_at,
            source_updated_at
        from source
    ),

    profissional_renomeado as (
        select
            {{ process_null("nome") }} as nome,
            {{ process_null("cns") }} as cns,
            {{ process_null("cpf") }} as cpf,
            {{ process_null("cbo") }} as id_cbo,
            -- source_updated_at as updated_at -- Estão mandando este campo vazio
            datetime(datalake_loaded_at, 'America/Sao_Paulo') as loaded_at,
            date(datalake_loaded_at, 'America/Sao_Paulo') as data_particao
        from profissional_json
        qualify row_number() over (partition by cpf order by datalake_loaded_at) = 1
    )

select *
from profissional_renomeado
