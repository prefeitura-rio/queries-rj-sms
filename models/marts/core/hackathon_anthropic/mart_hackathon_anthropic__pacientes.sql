{{
    config(
        alias="pacientes"
    )
}}

with 

hipertensos as (
  select distinct
    a.patient_cpf as cpf,
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
    a.patient_cpf as cpf,
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
    cpf,
    data_inicio
  from {{ ref('mart_bi_gestacoes__gestacoes') }}
  where
    cpf is not null
    and cpf in (
      select original.cpf from cadastros_elegiveis
    )
    and (
      extract(year from data_inicio) = 2025
    )
),

ultima_gestacao_do_paciente as (
  select 
    cpf,
    format_date('%Y-%m', data_inicio) as ultima_gestacao_mes_inicio
  from gestacoes
  qualify row_number() over (
    partition by cpf
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
    left join hipertensos h on h.cpf = c.original.cpf
    left join diabeticos d on d.cpf = c.original.cpf
    left join ultima_gestacao_do_paciente g on g.cpf = c.original.cpf
)

select
  distinct
  c.paciente_id,
  c.equipe_id,
  c.unidade_id,

  faixa_etaria,
  sexo,
  raca_cor,
  situacao_vulnerabilidade,

  ST_X(c.endereco) as endereco_longitude,
  ST_Y(c.endereco) as endereco_latitude,

  coalesce(condicoes.hipertenso, false) as hipertenso,
  coalesce(condicoes.diabetico, false) as diabetico,

  condicoes.ultima_gestacao_mes_inicio

from cadastros_elegiveis c
  left join condicoes using (paciente_id)