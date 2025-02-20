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
  calculando_permissoes as (
    SELECT
      *,
      CASE
        WHEN (funcao_grupo = 'MEDICOS' and unidade_tipo in ('UPA','HOSPITAL', 'CER', 'CCO','MATERNIDADE')) 
        THEN 'full_permission'

        WHEN (funcao_grupo = 'MEDICOS' and unidade_tipo in ('CMS','POLICLINICA','CF','CMR','CSE'))
        THEN 'only_from_same_unit'

        WHEN (funcao_grupo = 'ENFERMEIROS')
        THEN 'only_from_same_unit' 

        ELSE null
      END as nivel_acesso
    from uniao
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

