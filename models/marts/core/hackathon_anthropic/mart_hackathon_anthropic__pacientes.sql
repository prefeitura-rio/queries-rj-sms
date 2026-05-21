{{
    config(
        alias="pacientes"
    )
}}

with 

hipertensos as (
  select distinct
    a.patient_cpf as paciente_id,
    'hipertenso' as condicao
  from {{ ref('raw_prontuario_vitacare_historico__condicao') }} c
    inner join {{ ref('raw_prontuario_vitacare_historico__acto') }} a
      using (id_prontuario_global)
  where
    regexp_contains(cod_cid10, r'^(I1[0-35])')
    and a.unidade_ap = '22'
    and a.patient_cpf is not null
),

diabeticos as (
  select distinct
    a.patient_cpf as paciente_id,
    'diabetico' as condicao
  from {{ ref('raw_prontuario_vitacare_historico__condicao') }} c
    inner join {{ ref('raw_prontuario_vitacare_historico__acto') }} a
      using (id_prontuario_global)
  where
    regexp_contains(cod_cid10, r'^(E1[0-4])')
    and a.unidade_ap = '22'
    and a.patient_cpf is not null
),

cadastros_elegiveis as (
  select *
  from {{ ref('mart_hackathon_anthropic__elegiveis') }}
),

gestacoes as (
  select
    cpf as paciente_id,
    data_inicio,
    data_fim_efetiva
  from {{ ref('mart_bi_gestacoes__gestacoes') }}
  where
    cpf is not null
    and cpf in (
      select paciente_id from cadastros_elegiveis
    )
    and (
      extract(year from data_inicio) = 2025
      or extract(year from data_fim_efetiva) = 2025
    )
),

ultima_gestacao_do_paciente as (
  select 
    paciente_id,
    format_date('%Y-%m', data_inicio) as ultima_gestacao_mes_inicio
  from gestacoes
  qualify row_number() over (
    partition by paciente_id
    order by data_inicio desc
  ) = 1
),

condicoes as (
  select
    c.paciente_id,
    h.condicao is not null as hipertenso,
    d.condicao is not null as diabetico,
    g.ultima_gestacao_mes_inicio,
  from cadastros_elegiveis c 
    left join hipertensos h using (paciente_id)
    left join diabeticos d using (paciente_id)
    left join ultima_gestacao_do_paciente g using (paciente_id)
)

select
  distinct
  c.paciente_id,
  c.equipe_id,
  c.unidade_id,

  faixa_etaria,
  sexo,
  raca_cor,
  escolaridade,
  territorio_social,
  vulnerabilidade_social,

  ST_X(c.endereco) as endereco_longitude,
  ST_Y(c.endereco) as endereco_latitude,

  coalesce(condicoes.hipertenso, false) as hipertenso,
  coalesce(condicoes.diabetico, false) as diabetico,

  condicoes.ultima_gestacao_mes_inicio

from cadastros_elegiveis c
  left join condicoes using (paciente_id)