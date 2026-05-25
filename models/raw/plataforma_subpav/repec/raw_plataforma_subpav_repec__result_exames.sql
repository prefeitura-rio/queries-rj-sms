{{
    config(
        alias="repec__result_exames",
        materialized="table",
        tags=["subpav", "repec"]
    )
}}

with
    source as (
        select *
        from {{ source("brutos_plataforma_subpav_staging", "subpav_repec__result_exames") }}
    ),

    tratar_campos as (
        select
            {{ process_null('id_resultado_exame') }} as id_resultado_exame,
            {{ process_null('id_atendimento') }} as id_atendimento,
            {{ process_null('id_exame') }} as id_exame,
            {{ process_null('parametro') }} as parametro,
            {{ process_null('data_exame') }} as data_exame,
            {{ process_null('resultado') }} as resultado,
            {{ process_null('unidade_medida') }} as unidade_medida,
            {{ process_null('data_registro') }} as data_registro,
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
                    id_resultado_exame,
                    id_atendimento,
                    id_exame,
                    parametro,
                    data_exame,
                    resultado,
                    unidade_medida,
                    data_registro
            order by
                    datalake_loaded_at desc,
                    origem_arquivo desc
        ) = 1
    )

select *
from deduplicar
