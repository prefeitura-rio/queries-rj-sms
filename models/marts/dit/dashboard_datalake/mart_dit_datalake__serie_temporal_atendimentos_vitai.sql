{{
    config(
        alias='serie_temporal_atendimentos_vt',
        materialized='incremental',
        incremental_strategy='insert_overwrite',
        partition_by={
            "field": "data_registro",
            "data_type": "date",
            "granularity": "month"
        },
        unique_key=['data_registro'],
        description='Série temporal de atendimentos por data de entrada no prontuário Vitai',
        tags=['datalake']
    )
}}

{% set last_partition = get_last_partition_date(this) %}

select 
    cast(data_entrada as date) as data_registro,
    count(gid) as atendimentos
from {{ ref('raw_prontuario_vitai__boletim') }}
{% if is_incremental() %}
    where cast(data_entrada as date) >= date('{{ last_partition }}')
{% endif %}
group by 1