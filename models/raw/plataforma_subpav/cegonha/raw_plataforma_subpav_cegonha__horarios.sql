{{ config(
    schema = 'brutos_plataforma_subpav',
    alias = 'horarios',
    materialized = 'table'
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

)

select
    safe_cast({{ normalize_null("trim(id_horario)") }} as int64) as id_horario,
    {{ normalize_null("trim(nme_horario)") }} as nme_horario,
    safe_cast({{ normalize_null("trim(flg_ativo)") }} as int64) as flg_ativo,
    safe_cast({{ normalize_null("trim(created)") }} as datetime) as created_at,
    safe_cast({{ normalize_null("trim(modified)") }} as datetime) as updated_at,
    safe_cast({{ normalize_null("trim(datalake_loaded_at)") }} as timestamp) as datalake_loaded_at
from base