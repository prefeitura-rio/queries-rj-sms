{{
    config(
        alias="repec__medicamentos_dispensados",
        materialized="table",
        tags=["subpav", "repec"]
    )
}}

with
    source as (
        select *
        from {{ source("brutos_plataforma_subpav_staging", "subpav_repec__medicamentos_dispensados") }}
    ),

    tratar_campos as (
        select
            {{ process_null('id_dispensa_med') }} as id_dispensa_med,
            {{ process_null('id_paciente') }} as id_paciente,
            {{ process_null('id_profissional') }} as id_profissional,
            {{ process_null('id_medicamento') }} as id_medicamento,
            {{ process_null('quant_dispensada') }} as quant_dispensada,
            {{ process_null('id_prescricao_med') }} as id_prescricao_med,
            {{ process_null('data_dispensa') }} as data_dispensa,
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
                    id_dispensa_med,
                    id_paciente,
                    id_profissional,
                    id_medicamento,
                    quant_dispensada,
                    id_prescricao_med,
                    data_dispensa,
                    data_registro
            order by
                    datalake_loaded_at desc,
                    origem_arquivo desc
        ) = 1
    )

select *
from deduplicar
