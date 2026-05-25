{{ config(
    schema = 'brutos_plataforma_subpav',
    alias = 'horarios',
    materialized = 'table',
    meta={"owner": "karen"}
) }}

with source as (

    select *
    from {{ source('brutos_plataforma_subpav_staging', 'subpav_cegonha__horarios') }}

),

base as (

    select
        id_horario,
        nme_horario,
        flg_ativo,
        created,
        modified,
        datalake_loaded_at
    from source

),

dados_limpos as (

    select
        {{ normalize_null("trim(id_horario)") }} as id_horario,
        {{ normalize_null("trim(nme_horario)") }} as nme_horario,
        {{ normalize_null("trim(flg_ativo)") }} as flg_ativo,
        {{ normalize_null("trim(created)") }} as created,
        {{ normalize_null("trim(modified)") }} as modified,
        {{ normalize_null("trim(datalake_loaded_at)") }} as datalake_loaded_at
    from base

),

deduplicado as (

    select *
    from dados_limpos
    qualify row_number() over (
        partition by
            id_horario,
            nme_horario,
            flg_ativo,
            created,
            modified
        order by safe_cast(datalake_loaded_at as timestamp) desc
    ) = 1

)

select
    safe_cast(id_horario as int64) as id_horario,
    nme_horario,
    safe_cast(flg_ativo as int64) as flg_ativo,
    safe_cast(created as datetime) as created_at,
    safe_cast(modified as datetime) as updated_at,
    safe_cast(datalake_loaded_at as timestamp) as datalake_loaded_at
from deduplicado