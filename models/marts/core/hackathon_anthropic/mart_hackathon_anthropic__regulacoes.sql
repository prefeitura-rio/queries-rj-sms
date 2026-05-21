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
)

select
  {{ anonimize('m.paciente_cpf', "'hackathon_anthropic'") }} as paciente_id,
  coalesce(m.procedimento_sigtap, m.procedimento_interno) as procedimento,
  greatest(
    least(
      date_add(date(m.data_marcacao), interval e.shift_dias day),
      (select data_maxima from constantes)
    ),
    (select data_minima from constantes)
  ) as data_marcacao
from {{ ref('raw_sisreg_api__marcacoes') }} m
  inner join elegiveis e on m.paciente_cpf = e.cpf
  cross join constantes
where
  m.data_marcacao between '2025-01-01' and '2025-12-31'
  and m.paciente_cpf is not null
