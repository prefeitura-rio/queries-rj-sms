{{
    config(
        alias='serie_temporal_atendimentos_unidade_vt',
        materialized='incremental',
        incremental_strategy='insert_overwrite',
        partition_by={
            "field": "data_registro",
            "data_type": "date",
            "granularity": "month"
        },
        unique_key=['cnes', 'nome', 'data_registro'],
        description='Série temporal de atendimentos por data de entrada no prontuário Vitai, segmentada por unidade de saúde',
        tags=['datalake'],
    )
}}

{% set last_partition = get_last_partition_date(this) %}

with
  estabelecimentos as (
    select distinct gid, cnes, nome_estabelecimento as nome
    from {{ref('raw_prontuario_vitai__m_estabelecimento')}}
  ),

  atendimentos as (
    select
      a.gid as id_atendimento,
      gid_estabelecimento,
      cnes,
      {{ proper_estabelecimento('nome') }} as nome,
      cast(inicio_datahora as date) as data_registro,
    from {{ref('raw_prontuario_vitai__atendimento')}} a
    inner join estabelecimentos e on a.gid_estabelecimento = e.gid
    {% if is_incremental() %}
        where cast(inicio_datahora as date) >= date('{{ last_partition }}')
    {% endif %}
  )

select
  cnes,
  nome,
  data_registro,
  count(id_atendimento) as atendimentos
from atendimentos
group by 1,2,3