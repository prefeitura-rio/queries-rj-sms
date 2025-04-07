{{
    config(
        alias="acessos",
        schema="app_historico_clinico_treinamento",
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
    select *, 1 as prioridade from {{ ref('int_acessos__manual') }}
  ),

  -- -----------------------------------------
  -- Configurando Nivel de Acesso
  -- -----------------------------------------
  calculando_permissoes as (
    SELECT
      * except(nivel_de_acesso),
      'full_permission' as nivel_acesso
    from acessos_manual
  ),
  
  -- -----------------------------------------
  -- Removendo Duplicados
  -- -----------------------------------------
  ranked as (
    select
      *,
      row_number() over (partition by cpf order by prioridade desc) as rn
    from calculando_permissoes
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

select *
from particionado

