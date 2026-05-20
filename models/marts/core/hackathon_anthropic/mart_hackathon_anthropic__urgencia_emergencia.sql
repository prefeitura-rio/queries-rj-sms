{{
    config(
        alias="urgencia_emergencia"
    )
}}

with 

cadastros as (
  select
    cpf as paciente_id,
  from {{source('brutos_hackathon_anthropic','localizacao')}}
  where
    score > 0
)

select
  {{ anonimize('p.cpf', "'hackathon_anthropic'") }} as paciente_id,
  b.atendimento_tipo,
  b.data_entrada as atendido_em
from {{ ref('raw_prontuario_vitai__boletim') }} b
  inner join {{ ref('raw_prontuario_vitai__paciente') }} p
    on p.gid = b.gid_paciente
where
  p.cpf in (select paciente_id from cadastros)
  and p.cpf is not null
  and b.data_entrada between '2025-01-01' and '2025-12-31'
