{{
    config(
        alias="repec__agendamentos",
        materialized="table",
        tags=["subpav", "repec"]
    )
}}

with
    source as (
        select *
        from {{ source("brutos_plataforma_subpav_staging", "subpav_repec__agendamentos") }}
    ),

    tratar_campos as (
        select
            {{ process_null('id_agendamento') }} as id_agendamento,
            {{ process_null('cnes_entidade') }} as cnes_entidade,
            {{ process_null('id_paciente') }} as id_paciente,
            {{ process_null('id_profissional') }} as id_profissional,
            trim({{ remove_decode_chars_error("cast(" ~ process_null('id_tipo_atendimento_agdm') ~ " as string)") }}) as id_tipo_atendimento_agdm,
            trim({{ remove_decode_chars_error("cast(" ~ process_null('tipo_agdm') ~ " as string)") }}) as tipo_agdm,
            {{ process_null('estado_agdm') }} as estado_agdm,
            {{ process_null('data_registro_agdm') }} as data_registro_agdm,
            {{ process_null('id_prof_registro_agdm') }} as id_prof_registro_agdm,
            {{ process_null('data_marcacao') }} as data_marcacao,
            {{ process_null('data_chegada') }} as data_chegada,
            {{ process_null('id_prof_registro_chegada') }} as id_prof_registro_chegada,
            {{ process_null('data_efetuada') }} as data_efetuada,
            {{ process_null('id_prof_registro_efetuada') }} as id_prof_registro_efetuada,
            {{ process_null('data_cancelada') }} as data_cancelada,
            {{ process_null('id_prof_registro_cancelada') }} as id_prof_registro_cancelada,
            {{ process_null('data_faltou') }} as data_faltou,
            {{ process_null('id_prof_registro_faltou') }} as id_prof_registro_faltou,
            {{ process_null('motivo_falta') }} as motivo_falta,
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
                    id_agendamento,
                    cnes_entidade,
                    id_paciente,
                    id_profissional,
                    id_tipo_atendimento_agdm,
                    tipo_agdm,
                    estado_agdm,
                    data_registro_agdm,
                    id_prof_registro_agdm,
                    data_marcacao,
                    data_chegada,
                    id_prof_registro_chegada,
                    data_efetuada,
                    id_prof_registro_efetuada,
                    data_cancelada,
                    id_prof_registro_cancelada,
                    data_faltou,
                    id_prof_registro_faltou,
                    motivo_falta
            order by
                    datalake_loaded_at desc,
                    origem_arquivo desc
        ) = 1
    )

select *
from deduplicar
