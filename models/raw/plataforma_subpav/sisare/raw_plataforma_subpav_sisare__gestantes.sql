{{ config(
    schema = 'brutos_plataforma_subpav',
    alias = 'sisare__gestantes',
    materialized = 'table'
) }}

with source as (

    select *
    from {{ source('brutos_plataforma_subpav_staging', 'subpav_altas_referenciadas__gestantes') }}

),

base as (

    select
        id_gestante,
        id_paciente,
        id_internacao,
        ig,
        id_tipo_gravidez,
        id_via_parto,
        dt_parto,
        id_desfecho_internacao,
        id_desfecho_gestacao,
        status,
        created_at,
        updated_at,
        puerpera,
        datalake_loaded_at
    from source

),

dados_limpos as (

    select
        {{ normalize_null("trim(id_gestante)") }} as id_gestante,
        {{ normalize_null("trim(id_paciente)") }} as id_paciente,
        {{ normalize_null("trim(id_internacao)") }} as id_internacao,
        {{ normalize_null("regexp_replace(trim(ig), r'\\.0$', '')") }} as ig,
        {{ normalize_null("regexp_replace(trim(id_tipo_gravidez), r'\\.0$', '')") }} as id_tipo_gravidez,
        {{ normalize_null("regexp_replace(trim(id_via_parto), r'\\.0$', '')") }} as id_via_parto,
        {{ normalize_null("trim(dt_parto)") }} as dt_parto,
        {{ normalize_null("regexp_replace(trim(id_desfecho_internacao), r'\\.0$', '')") }} as id_desfecho_internacao,
        {{ normalize_null("regexp_replace(trim(id_desfecho_gestacao), r'\\.0$', '')") }} as id_desfecho_gestacao,
        {{ normalize_null("trim(status)") }} as status,
        {{ normalize_null("trim(created_at)") }} as created_at,
        {{ normalize_null("trim(updated_at)") }} as updated_at,
        {{ normalize_null("regexp_replace(trim(puerpera), r'\\.0$', '')") }} as puerpera,
        {{ normalize_null("trim(datalake_loaded_at)") }} as datalake_loaded_at
    from base

),

deduplicado as (

    select *
    from dados_limpos
    qualify row_number() over (
        partition by
            id_gestante,
            id_paciente,
            id_internacao,
            ig,
            id_tipo_gravidez,
            id_via_parto,
            dt_parto,
            id_desfecho_internacao,
            id_desfecho_gestacao,
            status,
            created_at,
            updated_at,
            puerpera
        order by safe_cast(datalake_loaded_at as timestamp) desc
    ) = 1

)

select
    id_gestante,
    id_paciente,
    id_internacao,
    safe_cast(ig as int64) as ig,
    safe_cast(id_tipo_gravidez as int64) as id_tipo_gravidez,
    safe_cast(id_via_parto as int64) as id_via_parto,
    safe.parse_date('%Y-%m-%d', dt_parto) as dt_parto,
    safe_cast(id_desfecho_internacao as int64) as id_desfecho_internacao,
    safe_cast(id_desfecho_gestacao as int64) as id_desfecho_gestacao,
    status,
    safe.parse_datetime('%Y-%m-%d %H:%M:%S', created_at) as created_at,
    safe.parse_datetime('%Y-%m-%d %H:%M:%S', updated_at) as updated_at,
    safe_cast(puerpera as int64) as puerpera,
    safe_cast(datalake_loaded_at as timestamp) as datalake_loaded_at
from deduplicado