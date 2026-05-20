{{
    config(
        alias="urgencia_emergencia"
    )
}}

with 

cadastros as (
  select
    original.cpf
  from {{ref('mart_hackathon_anthropic__elegiveis')}}
)

select
  {{ anonimize('p.cpf', "'hackathon_anthropic'") }} as paciente_id,
  b.atendimento_tipo,
  b.data_entrada as atendido_em
from {{ ref('raw_prontuario_vitai__boletim') }} b
  inner join {{ ref('raw_prontuario_vitai__paciente') }} p
    on p.gid = b.gid_paciente
where
  p.cpf in (select cpf from cadastros)
  and p.cpf is not null
  and b.data_entrada between '2025-01-01' and '2025-12-31'
