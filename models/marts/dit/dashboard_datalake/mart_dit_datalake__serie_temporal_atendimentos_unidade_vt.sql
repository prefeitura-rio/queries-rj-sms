{{
    config(
        alias='serie_temporal_atendimentos_unidade_vt',
        materialized='table',
    )
}}

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
    where cast(inicio_datahora as date) between DATE_SUB(CURRENT_DATE('America/Sao_Paulo') - 1, INTERVAL 30 DAY) and CURRENT_DATE('America/Sao_Paulo') - 1
  )

select
  cnes,
  nome,
  data_registro,
  count(id_atendimento) as atendimentos
from atendimentos
group by 1,2,3
order by 3