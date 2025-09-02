{{
    config(
        schema="brutos_sheets",
        alias="material_cobertura_programas",
        materialized="table",
        tag=["daily", "subgeral", "cnes_subgeral", "monitora_reg"],
    )
}}
-- TODO: conferir tags acima

with source as (
      select * from {{ source('brutos_sheets_staging', 'material_cobertura_programas') }}
),

renamed as (
    select
        -- estabelecimento
        id_cnes,
        nome as estabelecimento_nome,
        ap as estabelecimento_ap,

        -- programas
        tarv as tarv_indicador,
        prep as prep_indicador,
        pep as pep_indicador,
        talidomida as talidomida_indicador,
        hepatite as hepatite_indicador,

    from source
),

fixed as (
    select
        * except (
            id_cnes,
            tarv_indicador,
            prep_indicador,
            pep_indicador,
            talidomida_indicador,
            hepatite_indicador
        ),

        -- fix numeric columns
        {{ clean_numeric_string("id_cnes") }} as id_cnes,

        -- fix boolean columns
        if (lower(tarv_indicador) = "sim", true, false) as tarv_indicador,
        if (lower(prep_indicador) = "sim", true, false) as prep_indicador,
        if (lower(pep_indicador) = "sim", true, false) as pep_indicador,
        if (lower(talidomida_indicador) = "sim", true, false) as talidomida_indicador,
        if (lower(hepatite_indicador) = "sim", true, false) as hepatite_indicador,
    from renamed
),

final as (
    select

        -- estabelecimento
        id_cnes,
        estabelecimento_nome,
        estabelecimento_ap,

        -- programas
        tarv_indicador,
        prep_indicador,
        pep_indicador,
        talidomida_indicador,
        hepatite_indicador,
    from fixed
)

select * from final