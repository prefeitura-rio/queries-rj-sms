{{
    config(
        alias="regulacoes"
    )
}}

with

unidades_da_22 as (
  select id_cnes
  from {{ ref('dim_estabelecimento') }}
  where area_programatica = '22'
)

select
  SHA256(paciente_cpf) as paciente_id,
  coalesce(procedimento_sigtap, procedimento_interno) as procedimento,
  data_solicitacao,
  data_marcacao
from {{ ref('raw_sisreg_api__marcacoes') }}
where
  unidade_solicitante_id in (select id_cnes from unidades_da_22)
  and data_solicitacao between '2025-01-01' and '2025-12-31'
  and paciente_cpf is not null
