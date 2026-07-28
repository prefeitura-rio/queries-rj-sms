{{
    config(
        alias='serie_temporal_atendimentos_vitacare',
        materialized='incremental',
        incremental_strategy='insert_overwrite',
        partition_by={
            "field": "data_registro",
            "data_type": "date",
            "granularity": "month"
        },
        unique_key=['data_registro'],
        description='Série temporal de atendimentos por data de entrada no prontuário Vitacare'
    )
}}

{% set last_partition = get_last_partition_date(this) %}

select 
  {{ parse_and_filter_future_date('datahora_inicio')}} as data_registro,
  count(id_prontuario_global) as atendimentos
from {{ref('raw_prontuario_vitacare__atendimento')}}
{% if is_incremental() %}
    where {{ parse_and_filter_future_date('datahora_inicio') }} >= date('{{ last_partition }}')
{% endif %}
group by 1