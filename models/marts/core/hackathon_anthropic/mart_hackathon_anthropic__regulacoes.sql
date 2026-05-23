{{
    config(
        alias="regulacoes"
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
)

select
  {{ anonimize('m.paciente_cpf', "'hackathon_anthropic'") }} as paciente_id,
  REGEXP_EXTRACT(TRIM(coalesce(m.procedimento_sigtap, m.procedimento_interno)), r'^(\S+)') AS procedimento,
  date_add(date(m.data_marcacao), interval e.shift_dias day) as data_marcacao
from {{ ref('raw_sisreg_api__marcacoes') }} m
  inner join elegiveis e on m.paciente_cpf = e.cpf
  inner join procedimentos_comuns pc on coalesce(m.procedimento_sigtap, m.procedimento_interno) = pc.procedimento
  cross join constantes
where
  m.paciente_cpf is not null
  and date_add(date(m.data_marcacao), interval e.shift_dias day) between (select data_minima from constantes) and (select data_maxima from constantes)
