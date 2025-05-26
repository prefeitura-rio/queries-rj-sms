{{
    config(
        alias="acessos",
        schema="app_historico_clinico",
        materialized="table",
        partition_by={
            "field": "cpf_particao",
            "data_type": "int64",
            "range": {"start": 0, "end": 100000000000, "interval": 34722222},
        },
    )
}}

with
  -- -----------------------------------------
  -- Dados de Acesso: Manual (Sheets) e Automatico (Ergon+CNES)
  -- -----------------------------------------
  acessos_manual as (
    select * from {{ ref('int_acessos__manual') }}
  ),
  acessos_automatico as (
    select * from {{ ref('int_acessos__automatico') }}
  ),

  -- -----------------------------------------
  -- Unindo Dados de Acesso
  -- -----------------------------------------
  uniao as (
    select *
    from acessos_manual

    union all

    select *
    from acessos_automatico
  ),

  -- -----------------------------------------
  -- Dados de Vinculos
  -- -----------------------------------------
  vinculos_uniao as (
    select * except(vinculos)
    from uniao, unnest(vinculos) as vinculo
  ),

  -- -----------------------------------------
  -- Configurando Nivel de Acesso
  -- -----------------------------------------
  calculando_permissoes as (
    SELECT
      * except(nivel_de_acesso),
      nivel_de_acesso as nivel_acesso
    from vinculos_uniao
  ),
  
  -- -----------------------------------------
  -- Agrupando
  -- -----------------------------------------
  agrupando as (
    select
      cpf,
      nome_completo,
      array_agg(
        struct(
          unidade_nome,
          unidade_tipo,
          unidade_cnes,
          unidade_ap,
          funcao_detalhada,
          funcao_grupo,
          nivel_acesso
        )
      ) as vinculos
    from calculando_permissoes
    group by 1, 2
  ),

  -- -----------------------------------------
  -- Partição
  -- -----------------------------------------
  particionado as (
    select 
      safe_cast(cpf as int64) as cpf_particao,
      *
    from agrupando
  )

select *
from particionado
