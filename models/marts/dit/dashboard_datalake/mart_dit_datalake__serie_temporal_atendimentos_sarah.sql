{{
    config(
        alias='serie_temporal_atendimentos_sarah',
        materialized='incremental',
        incremental_strategy='insert_overwrite',
        partition_by={
            "field": "data_registro",
            "data_type": "date",
            "granularity": "month"
        },
        unique_key=['data_registro'],
        description='Série temporal de atendimentos por data de entrada no prontuário Sarah',
        tags=['datalake']
    )
}}

{% set last_partition = get_last_partition_date(this) %}

select
  {{ parse_and_filter_future_date('datahora_entrada') }} as data_registro,
  count(atendimento_numero) as atendimento
from {{ ref('raw_prontuario_sarah__atendimento') }}
{% if is_incremental() %}
    where {{ parse_and_filter_future_date('data_entrada') }} >= date('{{ last_partition }}')
{% endif %}
group by 1
