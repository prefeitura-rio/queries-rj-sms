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

cadastros_com_endereco as (
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
      select paciente_id from cadastros_com_endereco
    )
    and (
      extract(year from data_inicio) = 2025
      or extract(year from data_fim_efetiva) = 2025
    )
),

ultima_gestacao_do_paciente as (
  select 
    paciente_id,
    format_date('%Y-%m', data_inicio) as ultima_gestacao_mes_inicio,
    format_date('%Y-%m', data_fim_efetiva) as ultima_gestacao_mes_fim
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
    g.ultima_gestacao_mes_fim
  from cadastros_com_endereco c 
    left join hipertensos h using (paciente_id)
    left join diabeticos d using (paciente_id)
    left join ultima_gestacao_do_paciente g using (paciente_id)
),

pacientes_randomizados as (
  select
    *,
    row_number() over (
      partition by equipe_id
      order by rand()
    ) as endereco_random_id
  from cadastros_com_endereco
),

enderecos_randomizados as (
  select
    equipe_id,
    endereco_latitude,
    endereco_longitude,
    endereco_score,
    row_number() over (
      partition by equipe_id
      order by rand()
    ) as endereco_random_id
  from cadastros_com_endereco
)

select
  distinct
  p.paciente_id,
  p.equipe_id,
  p.unidade_id,

  p.faixa_etaria,
  p.sexo,
  p.raca_cor,
  p.nacionalidade,
  p.escolaridade,
  p.territorio_social,
  p.vulnerabilidade_social,

  coalesce(condicoes.hipertenso, false) as hipertenso,
  coalesce(condicoes.diabetico, false) as diabetico,

  condicoes.ultima_gestacao_mes_inicio,
  condicoes.ultima_gestacao_mes_fim,

  e.endereco_longitude,
  e.endereco_latitude,
  e.endereco_score

from pacientes_randomizados p
  left join condicoes
    using (paciente_id)
  left join enderecos_randomizados e
    on p.equipe_id = e.equipe_id
    and p.endereco_random_id = e.endereco_random_id