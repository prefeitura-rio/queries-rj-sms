{{
    config(
        alias="repec__resultados_testes_rapidos",
        materialized="table",
        tags=["subpav", "repec"]
    )
}}

with
    source as (
        select *
        from {{ source("brutos_plataforma_subpav_staging", "subpav_repec__resultados_testes_rapidos") }}
    ),

    tratar_campos as (
        select
            {{ process_null('id_result_teste_rapido') }} as id_result_teste_rapido,
            {{ process_null('id_atendimento') }} as id_atendimento,
            trim({{ remove_decode_chars_error("cast(" ~ process_null('id_tipo_teste_rapido') ~ " as string)") }}) as id_tipo_teste_rapido,
            trim({{ remove_decode_chars_error("cast(" ~ process_null('result_texto') ~ " as string)") }}) as result_texto,
            {{ process_null('result_num') }} as result_num,
            {{ process_null('obs_teste_rapido') }} as obs_teste_rapido,
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
                    id_result_teste_rapido,
                    id_atendimento,
                    id_tipo_teste_rapido,
                    result_texto,
                    result_num,
                    obs_teste_rapido,
                    data_registro
            order by
                    datalake_loaded_at desc,
                    origem_arquivo desc
        ) = 1
    )

select *
from deduplicar
