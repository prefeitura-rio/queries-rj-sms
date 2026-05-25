{{ config( 
    schema = 'brutos_plataforma_subpav',
    alias = 'unidades_referencia_encaminha',
    materialized = 'table',
    meta={"owner": "karen"}
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

),

dados_limpos as (

    select
        {{ normalize_null("trim(id_unidades_referencia_encaminha)") }} as id_unidades_referencia_encaminha,
        {{ normalize_null("trim(num_cnes_referencia)") }} as num_cnes_referencia,
        {{ normalize_null("trim(num_cnes_encaminha)") }} as num_cnes_encaminha,
        {{ normalize_null("trim(flg_ativo)") }} as flg_ativo,
        {{ normalize_null("trim(created)") }} as created,
        {{ normalize_null("trim(modified)") }} as modified,
        {{ normalize_null("trim(flg_prenatal_maternidade)") }} as flg_prenatal_maternidade,
        {{ normalize_null("trim(datalake_loaded_at)") }} as datalake_loaded_at
    from base

),

deduplicado as (

    select *
    from dados_limpos
    qualify row_number() over (
        partition by
            id_unidades_referencia_encaminha,
            num_cnes_referencia,
            num_cnes_encaminha,
            flg_ativo,
            created,
            modified,
            flg_prenatal_maternidade
        order by safe_cast(datalake_loaded_at as timestamp) desc
    ) = 1

)

select
    safe_cast(id_unidades_referencia_encaminha as int64) as id_unidades_referencia_encaminha,
    num_cnes_referencia,
    num_cnes_encaminha,
    safe_cast(flg_ativo as int64) as flg_ativo,
    safe_cast(created as datetime) as created_at,
    safe_cast(modified as datetime) as updated_at,
    safe_cast(flg_prenatal_maternidade as int64) as flg_prenatal_maternidade,
    safe_cast(datalake_loaded_at as timestamp) as datalake_loaded_at
from deduplicado