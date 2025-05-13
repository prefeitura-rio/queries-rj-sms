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
    select *, 2 as prioridade
    from acessos_manual

    union all

    select *, 1 as prioridade
    from acessos_automatico
  ),

  -- -----------------------------------------
  -- Configurando Nivel de Acesso
  -- -----------------------------------------
  busca_maior_prioridade as (
    select *
    from uniao
    qualify row_number() over (partition by cpf order by prioridade desc) = 1
  ),

  removendo_treinamento as (
    select *
    from busca_maior_prioridade
    where acesso.nivel_acesso_descricao != 'training'
  ),
  
  -- -----------------------------------------
  -- Removendo Duplicados
  -- -----------------------------------------
  ranked as (
    select
      *,
      row_number() over (partition by cpf order by prioridade desc) as rn
    from removendo_treinamento
  ),
  deduped as (
    select * except(rn, prioridade)
    from ranked
    where rn = 1
  ),

  -- -----------------------------------------
  -- Partição
  -- -----------------------------------------
  particionado as (
    select 
      safe_cast(cpf as int64) as cpf_particao,
      *
    from deduped
  )

select * except(acesso), acesso.nivel_acesso_descricao as nivel_acesso
from particionado

