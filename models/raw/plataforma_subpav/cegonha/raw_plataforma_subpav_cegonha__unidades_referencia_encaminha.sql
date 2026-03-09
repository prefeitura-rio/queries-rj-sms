{{ config(
    schema = 'brutos_plataforma_subpav',
    alias = 'unidades_referencia_encaminha',
    materialized = 'table'
) }}

with source as (

    select *
    from {{ source('brutos_plataforma_subpav_staging', 'subpav_cegonha__unidades_referencia_encaminha') }}

),

base as (

    select
        id_unidades_referencia_encaminha,
        num_cnes_referencia,
        num_cnes_encaminha,
        flg_ativo,
        created,
        modified,
        flg_prenatal_maternidade,
        datalake_loaded_at
    from source

)

select
    safe_cast(id_unidades_referencia_encaminha as int64) as id_unidades_referencia_encaminha,
    {{ normalize_null("trim(num_cnes_referencia)") }} as num_cnes_referencia,
    {{ normalize_null("trim(num_cnes_encaminha)") }} as num_cnes_encaminha,
    safe_cast(flg_ativo as int64) as flg_ativo,
    safe_cast({{ normalize_null("trim(created)") }} as datetime) as created_at,
    safe_cast({{ normalize_null("trim(modified)") }} as datetime) as updated_at,
    safe_cast(flg_prenatal_maternidade as int64) as flg_prenatal_maternidade,
    safe_cast(datalake_loaded_at as timestamp) as datalake_loaded_at
from base