{{
    config(
        alias="profissionais"
    )
}}

with

elegiveis as (
  select
    paciente_id,
    equipe_id,
    unidade_id
  from {{ ref('mart_hackathon_anthropic__elegiveis') }}
),

visitas as (
  select
    paciente_id,
    profissional_id,
    registrados_em
  from {{ ref('mart_hackathon_anthropic__visitas') }}
),

-- Pegar o vínculo mais recente por profissional
vinculos_com_data as (
  select
    e.unidade_id,
    e.equipe_id,
    v.profissional_id,
    v.registrados_em
  from visitas v inner join elegiveis e using(paciente_id)
)

select
  unidade_id,
  equipe_id,
  profissional_id
from vinculos_com_data
qualify row_number() over (
  partition by profissional_id
  order by registrados_em desc
) = 1
