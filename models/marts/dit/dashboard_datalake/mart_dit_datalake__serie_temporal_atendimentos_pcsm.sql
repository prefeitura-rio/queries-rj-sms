{{
    config(
        alias='serie_temporal_atendimentos_pcsm',
        materialized='incremental',
        incremental_strategy='merge',
        partition_by={
            "field": "data_registro",
            "data_type": "date",
            "granularity": "day"
        },
        cluster_by=['data_registro'],
        unique_key=['data_registro'],
        description='Série temporal de atendimentos por data de entrada no prontuário do PCSM',
        tags=['datalake']
    )
}}

select
    {{parse_and_filter_future_date('data_entrada_atendimento')}} as data_registro,
    count(id_atendimento) as atendimentos
from {{ ref('raw_pcsm_atendimentos') }}
{% if is_incremental() %}
    where {{parse_and_filter_future_date('data_entrada_atendimento')}} > (select max(data_registro) from {{ this }})
{% endif %}
group BY 1