{{
    config(
        alias="profissional"
    )
}}

with

constantes as (
  select
    date('2025-01-01') as data_minima,
    date('2025-12-31') as data_maxima
),

elegiveis as (
  select
    original.cpf,
    original.shift_dias,
    equipe_id,
    unidade_id
  from {{ ref('mart_hackathon_anthropic__elegiveis') }}
),

casos as (
  select
    {{ anonimize('profissional_cpf', "'hackathon_anthropic'") }} as profissional_id,
    e.equipe_id,
    e.unidade_id
  from {{ ref('raw_prontuario_vitacare_historico__acto') }} a
    inner join elegiveis e on a.patient_cpf = e.cpf
    cross join constantes
  where
    a.tipo_consulta = 'Visita Domiciliar'
    and a.profissional_cbo_descricao in ('Agente comunitário de saúde', 'Técnico em Agente Comunitário de Saúde')
    and a.patient_cpf is not null
    and a.profissional_equipe_cod_ine is not null
    and date_add(date(a.datahora_fim_atendimento), interval e.shift_dias day) between (select data_minima from constantes) and (select data_maxima from constantes)
),

contagem_por_profissional_equipe as (
  select
    profissional_id,
    equipe_id,
    unidade_id,
    count(*) as total_atendimentos
  from casos
  group by 1, 2, 3
),

equipe_mais_comum_por_profissional as (
  select
    profissional_id,
    equipe_id,
    unidade_id,
    total_atendimentos
  from contagem_por_profissional_equipe
  qualify row_number() over (
    partition by profissional_id
    order by total_atendimentos desc, equipe_id
  ) = 1
)

select
    distinct
    profissional_id,
    equipe_id,
    unidade_id
from equipe_mais_comum_por_profissional
