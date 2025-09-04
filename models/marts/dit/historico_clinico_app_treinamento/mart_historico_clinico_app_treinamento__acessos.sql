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
    select * from {{ ref('int_acessos__manual') }}
  ),

  -- -----------------------------------------
  -- Unindo Dados de Acesso
  -- -----------------------------------------
  uniao as (
    select *, 2 as prioridade
    from acessos_manual
  ),
  remove_vinculos_sem_acesso as (
      select
        cpf,
        nome_completo,
        prioridade,
        array_agg(
          struct(
            unidade_nome,
            unidade_tipo,
            unidade_cnes,
            unidade_ap,
            funcao_detalhada,
            funcao_grupo,
            'full_permission' as nivel_acesso,
            'full_permission' as granularidade_acesso
          )
        ) as vinculos
      from uniao as p, unnest(p.vinculos) as v
      where v.nivel_acesso is not null
      group by 1,2,3
  ),

  -- -----------------------------------------
  -- Configurando Nivel de Acesso
  -- -----------------------------------------
  busca_maior_prioridade as (
    select *
    from remove_vinculos_sem_acesso
    qualify row_number() over (partition by cpf order by prioridade desc) = 1
  ),

  -- -----------------------------------------
  -- Removendo Duplicados
  -- -----------------------------------------
  ranked as (
    select
      *,
      row_number() over (partition by cpf order by prioridade desc) as rn
    from busca_maior_prioridade
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
