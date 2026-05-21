{{
    config(
        alias="visitas"
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
    original.shift_dias
  from {{ ref('mart_hackathon_anthropic__elegiveis') }}
)

select
  {{ anonimize('profissional_cpf', "'hackathon_anthropic'") }} as profissional_id,
  {{ anonimize('profissional_equipe_cod_ine', "'hackathon_anthropic'") }} as equipe_id,
  {{ anonimize('a.patient_cpf', "'hackathon_anthropic'") }} as paciente_id,
  date_add(date(a.datahora_fim_atendimento), interval e.shift_dias day) as visitado_em
from {{ ref('raw_prontuario_vitacare_historico__acto') }} a
  inner join elegiveis e on a.patient_cpf = e.cpf
  cross join constantes
where
  a.tipo_consulta = 'Visita Domiciliar'
  and a.patient_cpf is not null
  and a.profissional_equipe_cod_ine is not null
  and date_add(date(a.datahora_fim_atendimento), interval e.shift_dias day) between (select data_minima from constantes) and (select data_maxima from constantes)
