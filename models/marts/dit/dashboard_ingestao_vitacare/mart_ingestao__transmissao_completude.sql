{{ 
    config(
        alias='transmissao_completude',
        materialized='table',
    ) 
}}

with
  -- -----------------------------
  -- Dados de Unidades
  -- -----------------------------
  unidades as (
    select
      id_cnes,
      area_programatica,
      nome_fantasia
    from {{ ref('dim_estabelecimento') }} est
    where est.prontuario_versao = 'vitacare'
      and est.prontuario_episodio_tem_dado = 'sim'
      and id_cnes is not null
  ),

  atendimentos as (
    select
      origem,
      cnes_unidade as id_cnes,
      cast(id_prontuario_local as INT64) as id_prontuario_local
    from {{ ref('raw_prontuario_vitacare__atendimento') }}
      -- Dados dos últimos 30 dias
      where data_particao >= DATE_SUB(CURRENT_DATE("America/Sao_Paulo"), INTERVAL 30 DAY)
      and data_particao < CURRENT_DATE("America/Sao_Paulo")
      and cnes_unidade is not null and cnes_unidade != ""
  ),

  atendimentos_agrupados as (
    select
      origem,
      id_cnes,
      MIN(id_prontuario_local) as id_minimo,
      MAX(id_prontuario_local) as id_maximo,
      MAX(id_prontuario_local) - MIN(id_prontuario_local) + 1 as qtd_esperada,
      COUNT(*) as qtd_real,
      (MAX(id_prontuario_local) - MIN(id_prontuario_local) + 1) - COUNT(*) as qtd_falta
    from atendimentos
    group by origem, id_cnes
  ),

  final as (
    select
      unidades.area_programatica,
      unidades.id_cnes,
      unidades.nome_fantasia,

      atendimentos_agrupados.origem,
      atendimentos_agrupados.id_minimo,
      atendimentos_agrupados.id_maximo,
      atendimentos_agrupados.qtd_esperada,
      atendimentos_agrupados.qtd_real,
      atendimentos_agrupados.qtd_falta,

      (atendimentos_agrupados.qtd_real / atendimentos_agrupados.qtd_esperada) as cobertura_pct
    from unidades
      left join atendimentos_agrupados using(id_cnes)
  )

select * from final
order by origem, cobertura_pct desc
