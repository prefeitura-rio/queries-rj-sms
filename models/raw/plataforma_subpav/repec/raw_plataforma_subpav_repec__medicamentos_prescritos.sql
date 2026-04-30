{{
    config(
        alias="repec__medicamentos_prescritos",
        materialized="table",
        tags=["subpav", "repec"]
    )
}}

with
    source as (
        select *
        from {{ source("brutos_plataforma_subpav_staging", "subpav_repec__medicamentos_prescritos") }}
    ),

    tratar_campos as (
        select
            {{ process_null('id_prescricao_med') }} as id_prescricao_med,
            {{ process_null('id_atendimento') }} as id_atendimento,
            {{ process_null('id_medicamento') }} as id_medicamento,
            {{ process_null('medicamento') }} as medicamento,
            {{ process_null('apresentacao') }} as apresentacao,
            {{ process_null('num_meses') }} as num_meses,
            {{ process_null('quant_mes') }} as quant_mes,
            {{ process_null('obs_posologia') }} as obs_posologia,
            {{ process_null('cronico') }} as cronico,
            {{ process_null('custo_unit') }} as custo_unit,
            {{ process_null('data_prescricao') }} as data_prescricao,
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
                    id_prescricao_med,
                    id_atendimento,
                    id_medicamento,
                    medicamento,
                    apresentacao,
                    num_meses,
                    quant_mes,
                    obs_posologia,
                    cronico,
                    custo_unit,
                    data_prescricao
            order by
                    datalake_loaded_at desc,
                    origem_arquivo desc
        ) = 1
    )

select *
from deduplicar
