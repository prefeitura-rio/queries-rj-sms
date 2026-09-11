{{
    config(
        alias="pacientes"
    )
}}

with 

hipertensos as (
  select distinct
    a.patient_cpf as paciente_cpf,
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
    a.patient_cpf as paciente_cpf,
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
  from {{ ref('mart_visitare_app__elegiveis') }}
),

gestacoes as (
  select
    cpf,
    data_inicio
  from {{ ref('mart_bi_gestacoes__gestacoes') }}
  where
    cpf is not null
    and cpf in (
      select paciente_cpf from cadastros_elegiveis
    )
),

ultima_gestacao_do_paciente as (
  select 
    cpf,
    data_inicio as ultima_gestacao_data_inicio
  from gestacoes
  qualify row_number() over (
    partition by cpf
    order by data_inicio desc
  ) = 1
),

condicoes as (
  select
    c.paciente_cpf,
    h.condicao is not null as hipertenso,
    d.condicao is not null as diabetico,
    g.ultima_gestacao_data_inicio
  from cadastros_elegiveis c 
    left join hipertensos h on h.paciente_cpf = c.paciente_cpf
    left join diabeticos d on d.paciente_cpf = c.paciente_cpf
    left join ultima_gestacao_do_paciente g on g.cpf = c.paciente_cpf
)

select
  distinct
  c.paciente_cpf,
  c.equipe_ine,
  c.unidade_cnes,

  data_nascimento,
  sexo,
  raca_cor,
  situacao_vulnerabilidade,

  c.endereco,

  coalesce(condicoes.hipertenso, false) as hipertenso,
  coalesce(condicoes.diabetico, false) as diabetico,
  
  condicoes.ultima_gestacao_data_inicio

from cadastros_elegiveis c
  left join condicoes using (paciente_cpf)