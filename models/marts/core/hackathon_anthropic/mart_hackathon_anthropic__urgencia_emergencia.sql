{{
    config(
        alias="urgencia_emergencia"
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
  {{ anonimize('p.cpf', "'hackathon_anthropic'") }} as paciente_id,
  b.atendimento_tipo,
  greatest(
    least(
      date_add(date(b.data_entrada), interval e.shift_dias day),
      (select data_maxima from constantes)
    ),
    (select data_minima from constantes)
  ) as atendido_em
from {{ ref('raw_prontuario_vitai__boletim') }} b
  inner join {{ ref('raw_prontuario_vitai__paciente') }} p
    on p.gid = b.gid_paciente
  inner join elegiveis e on p.cpf = e.cpf
  cross join constantes
where
  p.cpf is not null
  and b.data_entrada between '2025-01-01' and '2025-12-31'
