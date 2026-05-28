{{
    config(
        alias="eventos_clinicos"
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

procedimentos_comuns as (
  select
    coalesce(procedimento_sigtap, procedimento_interno) as procedimento,
    count(*) as total_ocorrencias
  from {{ ref('raw_sisreg_api__marcacoes') }} m
    inner join elegiveis e on m.paciente_cpf = e.cpf
    cross join constantes
  where
    date(data_marcacao) between (select data_minima from constantes) and (select data_maxima from constantes)
    and coalesce(procedimento_sigtap, procedimento_interno) is not null
  group by 1
  having count(*) > 1000
),

agendamentos as (
  select
    {{ anonimize('m.paciente_cpf', "'hackathon_anthropic'") }} as paciente_id,
    'agendamento' AS tipo,
    date_add(date(m.data_marcacao), interval e.shift_dias day) as data_referencia
  from {{ ref('raw_sisreg_api__marcacoes') }} m
    inner join elegiveis e on m.paciente_cpf = e.cpf
    inner join procedimentos_comuns pc on coalesce(m.procedimento_sigtap, m.procedimento_interno) = pc.procedimento
    cross join constantes
  where
    m.paciente_cpf is not null
    and date_add(date(m.data_marcacao), interval e.shift_dias day) between (select data_minima from constantes) and (select data_maxima from constantes)
),

urgencia_emergencia_internacao as (
  select
    {{ anonimize('p.cpf', "'hackathon_anthropic'") }} as paciente_id,
    'urgencia-emergencia-ou-internacao' as tipo,
    date_add(date(b.data_entrada), interval e.shift_dias day) as data_referencia
  from {{ ref('raw_prontuario_vitai__boletim') }} b
    inner join {{ ref('raw_prontuario_vitai__paciente') }} p
      on p.gid = b.gid_paciente
    inner join elegiveis e on p.cpf = e.cpf
    cross join constantes
  where
    p.cpf is not null
    and b.atendimento_tipo in ('EMERGENCIA   ', 'INTERNACAO   ')
    and date_add(date(b.data_entrada), interval e.shift_dias day) between (select data_minima from constantes) and (select data_maxima from constantes)
)

select *
from agendamentos

union ALL

select *
from urgencia_emergencia_internacao