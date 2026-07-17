{{
    config(
        alias='serie_temporal_atendimentos_unidade_sarah',
        materialized='incremental',
        incremental_strategy='insert_overwrite',
        partition_by={
            "field": "data_registro",
            "data_type": "date",
            "granularity": "day"
        },
        cluster_by=['data_registro'],
        unique_key=['cnes', 'data_registro'],
        description='Série temporal de atendimentos por data de entrada no prontuário Sarah, segmentada por unidade de saúde',
        tags=['datalake']
    )
}}

{% set last_partition = get_last_partition_date(this) %}

with

    atendimento_unidade as(
      select
        id_cnes,
        date(datahora_entrada) as data_registro,
        count(distinct atendimento_numero) as atendimentos
      from {{ ref('raw_prontuario_sarah__atendimento') }}
      group by 1,2
      {% if is_incremental() %}
          where cast(datahora_entrada as date) >= date('{{ last_partition }}')
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


