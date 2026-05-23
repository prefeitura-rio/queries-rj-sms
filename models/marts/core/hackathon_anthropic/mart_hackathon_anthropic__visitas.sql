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
),

-- Buscar todas as visitas domiciliares no período
visitas_brutas as (
  select
    a.profissional_cpf,
    a.patient_cpf,
    a.datahora_fim_atendimento,
    e.shift_dias
  from {{ ref('raw_prontuario_vitacare_historico__acto') }} a
    inner join elegiveis e on a.patient_cpf = e.cpf
    cross join constantes
  where
    a.tipo_consulta = 'Visita Domiciliar'
    and a.profissional_cbo_descricao in ('Agente comunitário de saúde', 'Técnico em Agente Comunitário de Saúde')
    and a.patient_cpf is not null
    and a.profissional_cpf is not null
    and date_add(date(a.datahora_fim_atendimento), interval e.shift_dias day)
      between (select data_minima from constantes) and (select data_maxima from constantes)
),

-- Aplicar shift e preparar dados
visitas_com_shift as (
  select
    profissional_cpf,
    patient_cpf,
    datetime_add(datahora_fim_atendimento, interval shift_dias day) as datahora_visita,
    date_add(date(datahora_fim_atendimento), interval shift_dias day) as data_visita
  from visitas_brutas
),

-- Remover duplicatas por profissional + paciente + data
-- Em caso de duplicata, mantém apenas uma ocorrência
visitas_deduplicadas as (
  select
    profissional_cpf,
    patient_cpf,
    datahora_visita,
    data_visita
  from visitas_com_shift
  qualify row_number() over (
    partition by profissional_cpf, patient_cpf, data_visita
    order by datahora_visita
  ) = 1
),

-- Anonimizar IDs
visitas_anonimizadas as (
  select
    {{ anonimize('profissional_cpf', "'hackathon_anthropic'") }} as profissional_id,
    {{ anonimize('patient_cpf', "'hackathon_anthropic'") }} as paciente_id,
    datahora_visita,
    data_visita
  from visitas_deduplicadas
),

-- Calcular ordem da visita por profissional no dia
visitas_com_ordem as (
  select
    profissional_id,
    paciente_id,
    data_visita as visitado_em,
    row_number() over (
      partition by profissional_id, data_visita
      order by datahora_visita
    ) as ordem_visita_dia
  from visitas_anonimizadas
)

select
  profissional_id,
  paciente_id,
  visitado_em,
  ordem_visita_dia
from visitas_com_ordem
