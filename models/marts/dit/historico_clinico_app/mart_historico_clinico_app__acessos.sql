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
  remove_sem_acesso_adiciona_equipe as (
    select
      p.cpf,
      p.nome_completo,
      p.prioridade,
      array_agg(
        struct(
          v.unidade_nome,
          v.unidade_tipo,
          v.unidade_cnes,
          v.unidade_ap,
          v.funcao_detalhada,
          v.funcao_grupo,
          v.nivel_acesso,
          v.granularidade_acesso,
          equipe.equipe_ine,
          equipe.equipe_nome
        )
      ) as vinculos
    from uniao as p, unnest(p.vinculos) as v
    left join {{ ref("int_cadastro__equipe_saude_familia") }} as equipe
      on (p.cpf = equipe.cpf and v.unidade_cnes = equipe.id_cnes)
    where v.nivel_acesso is not null
    group by 1,2,3
  ),

  -- -----------------------------------------
  -- Configurando Nivel de Acesso
  -- -----------------------------------------
  busca_maior_prioridade as (
    select *
    from remove_sem_acesso_adiciona_equipe
    qualify row_number() over (partition by cpf order by prioridade desc) = 1
  ),

  remove_treinamento as (
    select busca_maior_prioridade.*
    from busca_maior_prioridade, unnest(vinculos) as v
    where v.nivel_acesso != 'training'
  ),
  
  -- -----------------------------------------
  -- Removendo Duplicados
  -- -----------------------------------------
  ranked as (
    select
      *,
      row_number() over (partition by cpf order by prioridade desc) as rn
    from remove_treinamento
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
