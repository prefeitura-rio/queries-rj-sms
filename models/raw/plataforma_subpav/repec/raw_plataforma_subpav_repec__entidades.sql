{{
    config(
        alias="repec__entidades",
        materialized="table",
        tags=["subpav", "repec"]
    )
}}

with
    source as (
        select *
        from {{ source("brutos_plataforma_subpav_staging", "subpav_repec__entidades") }}
    ),

    tratar_campos as (
        select
            {{ process_null('cnes_entidade') }} as cnes_entidade,
            {{ process_null('designacao') }} as designacao,
            {{ process_null('morada') }} as morada,
            {{ process_null('cep') }} as cep,
            {{ process_null('municipio') }} as municipio,
            {{ process_null('cnpj') }} as cnpj,
            {{ process_null('cap') }} as cap,
            {{ process_null('cod_zona') }} as cod_zona,
            {{ process_null('data_inicio') }} as data_inicio,
            {{ process_null('origem_arquivo') }} as origem_arquivo,
            {{ process_null('origem_banco') }} as origem_banco,
            {{ repec_origem_unidade_para_cnes("origem_unidade") }} as cnes_origem,
            safe_cast({{ process_null('datalake_loaded_at') }} as timestamp) as datalake_loaded_at
        from source
    ),

    deduplicar as (
        select *
        from tratar_campos
        qualify row_number() over (
            partition by
                    cnes_entidade,
                    designacao,
                    morada,
                    cep,
                    municipio,
                    cnpj,
                    cap,
                    cod_zona,
                    data_inicio
            order by
                    datalake_loaded_at desc,
                    origem_arquivo desc
        ) = 1
    )

select *
from deduplicar
