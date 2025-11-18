{{
    config(
        alias="agendamento", 
        materialized="incremental",
        unique_key = ['id_global'],
        cluster_by= ['id_global'],
        schema="brutos_prontuario_vitacare_historico",
        partition_by={
            "field": "data_particao",
            "data_type": "date",
            "granularity": "day"
        }
    )
}}

{% set last_partition = get_last_partition_date(this) %}

with

    source_agendamento as (
        select *
        from {{ source('brutos_prontuario_vitacare_historico_staging', 'agendamentos') }} 
        {% if is_incremental() %}
        where data_particao > '{{ last_partition }}'
        {% endif %}
    ),


      -- Using window function to deduplicate agendamento
    agendamento_deduplicados as (
        select
            *
        from source_agendamento 
        qualify row_number() over (partition by source_id, id_cnes  order by extracted_at desc) = 1 
    ),

    fato_agendamento as (
        select
            replace({{ process_null('source_id') }}, '.0', '') as id_local,
            CONCAT(
                {{ process_null ('cnes')}},
                '.',
                replace({{ process_null('source_id') }}, '.0', '') 
            ) as id_global,
            CONCAT(
                {{ process_null ('cnes')}},
                '.',
                replace({{ process_null('ut_id') }}, '.0', '') 
            ) as id_cadastro,
            CONCAT(
                id_cnes,
                '.',
                replace({{ process_null('prof_id') }}, '.0', '')
            ) AS id_profissional,
            {{ process_null ('cnes')}} as id_cnes,
            
             safe_cast(concat({{ process_null('datahora_agendamento') }}, ':00') as datetime) as datahora_agendamento,
             safe_cast({{ process_null('datahora_marcacao_atendimento') }} as datetime) as datahora_marcacao_atendimento,
             safe_cast({{ process_null('source_updated_at') }} as datetime) as source_updated_at,
             {{ process_null('estado_marcacao') }} as estado_marcacao,
             {{ process_null('motivo') }} as motivo,
             {{ process_null('tipo_consulta') }} as tipo_consulta,
             {{ process_null('tipo_atendimento') }} as tipo_atendimento,
             {{ process_null('consulta_realizada') }} as consulta_realizada,

            extracted_at as loaded_at,
            date(safe_cast(extracted_at as datetime)) as data_particao
        from agendamento_deduplicados
    )

select
    *
from fato_agendamento