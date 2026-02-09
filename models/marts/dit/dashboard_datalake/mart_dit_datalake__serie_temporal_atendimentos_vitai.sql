{{
    config(
        alias='serie_temporal_atendimentos_vt',
        materialized='incremental',
        incremental_strategy='merge',
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

select 
    cast(data_entrada as date) as data_registro,
    count(gid) as atendimentos
from {{ ref('raw_prontuario_vitai__boletim') }}
{% if is_incremental() %}
    where cast(data_entrada as date) > (select max(data_registro) from {{ this }})
{% endif %}
group by 1