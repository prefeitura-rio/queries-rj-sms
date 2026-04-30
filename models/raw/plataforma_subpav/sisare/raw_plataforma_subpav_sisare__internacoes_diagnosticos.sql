{{ config(
    schema = 'brutos_plataforma_subpav',
    alias = 'sisare__internacoes_diagnosticos',
    materialized = 'table'
) }}

with source as (

    select *
    from {{ source('brutos_plataforma_subpav_staging', 'subpav_altas_referenciadas__internacoes_diagnosticos') }}

),

base as (

    select
        id_internacao_diagnostico,
        id_paciente,
        id_internacao,
        id_diagnostico,
        status,
        created_at,
        updated_at,
        datalake_loaded_at
    from source

),

dados_limpos as (

    select
        {{ normalize_null("trim(id_internacao_diagnostico)") }} as id_internacao_diagnostico,
        {{ normalize_null("trim(id_paciente)") }} as id_paciente,
        {{ normalize_null("trim(id_internacao)") }} as id_internacao,
        {{ normalize_null("regexp_replace(trim(id_diagnostico), r'\\.0$', '')") }} as id_diagnostico,
        {{ normalize_null("trim(status)") }} as status,
        {{ normalize_null("trim(created_at)") }} as created_at,
        {{ normalize_null("trim(updated_at)") }} as updated_at,
        {{ normalize_null("trim(datalake_loaded_at)") }} as datalake_loaded_at
    from base

),

deduplicado as (

    select *
    from dados_limpos
    qualify row_number() over (
        partition by id_internacao_diagnostico
        order by
            safe_cast(updated_at as timestamp) desc nulls last,
            safe_cast(created_at as timestamp) desc nulls last,
            safe_cast(datalake_loaded_at as timestamp) desc
    ) = 1

)

select
    safe_cast(id_internacao_diagnostico as int64) as id_internacao_diagnostico,
    safe_cast(id_paciente as int64) as id_paciente,
    safe_cast(id_internacao as int64) as id_internacao,
    safe_cast(id_diagnostico as int64) as id_diagnostico,
    safe_cast(status as int64) as status,
    safe_cast(created_at as timestamp) as created_at,
    safe_cast(updated_at as timestamp) as updated_at,
    safe_cast(datalake_loaded_at as timestamp) as datalake_loaded_at
from deduplicado