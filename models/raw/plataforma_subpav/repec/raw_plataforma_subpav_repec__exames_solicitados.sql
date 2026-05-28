{{
    config(
        alias="repec__exames_solicitados",
        materialized="table",
        tags=["subpav", "repec"]
    )
}}

with
    source as (
        select *
        from {{ source("brutos_plataforma_subpav_staging", "subpav_repec__exames_solicitados") }}
    ),

    tratar_campos as (
        select
            {{ process_null('id_exame_solicitado') }} as id_exame_solicitado,
            {{ process_null('id_atendimento') }} as id_atendimento,
            {{ process_null('procedimento_solicitado') }} as procedimento_solicitado,
            {{ process_null('quant_solicitada') }} as quant_solicitada,
            {{ process_null('custo_proc') }} as custo_proc,
            {{ process_null('apac') }} as apac,
            {{ process_null('proc_secundario_1') }} as proc_secundario_1,
            {{ process_null('proc_secundario_1_quant') }} as proc_secundario_1_quant,
            {{ process_null('proc_secundario_2') }} as proc_secundario_2,
            {{ process_null('proc_secundario_2_quant') }} as proc_secundario_2_quant,
            {{ process_null('proc_secundario_3') }} as proc_secundario_3,
            {{ process_null('proc_secundario_3_quant') }} as proc_secundario_3_quant,
            {{ process_null('diagnostico_associado') }} as diagnostico_associado,
            {{ process_null('diagnostico_secundario') }} as diagnostico_secundario,
            {{ process_null('diagnostico_outros') }} as diagnostico_outros,
            {{ process_null('laudo') }} as laudo,
            {{ process_null('observacoes') }} as observacoes,
            {{ process_null('dados_clinicos') }} as dados_clinicos,
            {{ process_null('material_a_examinar') }} as material_a_examinar,
            {{ process_null('data_solicitacao') }} as data_solicitacao,
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
                    id_exame_solicitado,
                    id_atendimento,
                    procedimento_solicitado,
                    quant_solicitada,
                    custo_proc,
                    apac,
                    proc_secundario_1,
                    proc_secundario_1_quant,
                    proc_secundario_2,
                    proc_secundario_2_quant,
                    proc_secundario_3,
                    proc_secundario_3_quant,
                    diagnostico_associado,
                    diagnostico_secundario,
                    diagnostico_outros,
                    laudo,
                    observacoes,
                    dados_clinicos,
                    material_a_examinar,
                    data_solicitacao
            order by
                    datalake_loaded_at desc,
                    origem_arquivo desc
        ) = 1
    )

select *
from deduplicar
