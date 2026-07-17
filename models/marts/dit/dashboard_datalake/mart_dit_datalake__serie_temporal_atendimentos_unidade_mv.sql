{{
    config(
        alias='serie_temporal_atendimentos_unidade_mv',
        materialized='incremental',
        incremental_strategy='insert_overwrite',
        partition_by={
            "field": "data_registro",
            "data_type": "date",
            "granularity": "day"
        },
        unique_key=['cnes', 'data_registro'],
        description='Série temporal de atendimentos por data de entrada no prontuário MV, segmentada por unidade de saúde',
        tags=['datalake']
    )
}}

{% set last_partition = get_last_partition_date(this) %}

with

    atendimento_unidade as(
      select
        id_cnes,
        date(atendimento_datahora) as data_registro,
        count(id_atendimento) as atendimentos
      from {{ ref('raw_prontuario_mv__atendimento') }}
      group by 1,2
      {% if is_incremental() %}
          where cast(atendimento_datahora as date) >= date('{{ last_partition }}')
      {% endif %}
    ),

    estabelecimentos as (
      select 
        id_cnes, 
        nome_acentuado as nome
      from {{ref('dim_estabelecimento')}}
    )

select 
    id_cnes as cnes,
    {{proper_estabelecimento('nome')}} as nome,
    data_registro,
    atendimentos
from atendimento_unidade a
inner join estabelecimentos using(id_cnes)


