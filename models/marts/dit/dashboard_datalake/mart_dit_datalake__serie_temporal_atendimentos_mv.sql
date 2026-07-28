{{
    config(
        alias='serie_temporal_atendimentos_mv',
        materialized='incremental',
        incremental_strategy='insert_overwrite',
        partition_by={
            "field": "data_registro",
            "data_type": "date",
            "granularity": "month"
        },
        unique_key=['data_registro'],
        description='Série temporal de atendimentos por data de entrada no prontuário MV'
    )
}}

{% set last_partition = get_last_partition_date(this) %}

select
  {{ parse_and_filter_future_date('atendimento_datahora') }} as data_registro,
  count(id_atendimento) as atendimentos
from {{ ref('raw_prontuario_mv__atendimento') }}
{% if is_incremental() %}
    where {{ parse_and_filter_future_date('atendimento_datahora') }} >= date('{{ last_partition }}')
{% endif %}
group by 1