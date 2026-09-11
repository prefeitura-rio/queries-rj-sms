{{
    config(
        alias="eventos_clinicos"
    )
}}

with

constantes as (
  select
    date('2025-01-01') as data_minima,
    date('2026-12-31') as data_maxima
),

elegiveis as (
  select
    paciente_cpf
  from {{ ref('mart_visitare_app__elegiveis') }}
),

agendamentos as (
  select
    e.paciente_cpf,
    'agendamento' AS tipo,
    date(m.data_marcacao) as data_referencia
  from {{ ref('raw_sisreg_api__marcacoes') }} m
    inner join elegiveis e on m.paciente_cpf = e.paciente_cpf
    cross join constantes
  where
    m.paciente_cpf is not null
    and date(m.data_marcacao) between (select data_minima from constantes) and (select data_maxima from constantes)
),

urgencia_emergencia_internacao as (
  select
    p.cpf as paciente_cpf,
    'urgencia-emergencia-ou-internacao' as tipo,
    date(b.data_entrada) as data_referencia
  from {{ ref('raw_prontuario_vitai__boletim') }} b
    inner join {{ ref('raw_prontuario_vitai__paciente') }} p
      on p.gid = b.gid_paciente
    inner join elegiveis e on p.cpf = e.paciente_cpf
    cross join constantes
  where
    p.cpf is not null
    and b.atendimento_tipo in ('EMERGENCIA   ', 'INTERNACAO   ')
    and date(b.data_entrada) between (select data_minima from constantes) and (select data_maxima from constantes)
)

select *
from agendamentos

union ALL

select *
from urgencia_emergencia_internacao