{{
    config(
        alias="regulacoes"
    )
}}

with

cadastros as (
  select
    cpf
  from {{ref('mart_hackathon_anthropic__elegiveis')}}
)

select
  {{ anonimize('paciente_cpf', "'hackathon_anthropic'") }} as paciente_id,
  coalesce(procedimento_sigtap, procedimento_interno) as procedimento,
  data_marcacao
from {{ ref('raw_sisreg_api__marcacoes') }}
where
  paciente_cpf in (select cpf from cadastros)
  and data_marcacao between '2025-01-01' and '2025-12-31'
  and paciente_cpf is not null
