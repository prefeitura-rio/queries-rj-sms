{{
    config(
        alias="sumario",
        materialized="table",
        cluster_by="cpf",
        schema="app_historico_clinico",
    )
}}

with
  base as (
    select cpf
    from {{ ref('mart_historico_clinico__paciente') }}
  ),
  alergias_grouped as (
    select
    paciente_cpf,
    alergias as allergies
    from {{ ref('mart_historico_clinico__alergia') }}
  ),
  medicamentos_cronicos_single as (
    select
        paciente_cpf,
        med.nome as nome_medicamento
    from {{ ref('mart_historico_clinico__medicamento_cronico') }},
        unnest(medicamentos) as med
  ),
  medicamentos_cronicos_grouped as (
    select
    paciente_cpf,
    array_agg(nome_medicamento) as continuous_use_medications
    from medicamentos_cronicos_single
    group by paciente_cpf
  )
select
    base.cpf,
    alergias_grouped.allergies,
    medicamentos_cronicos_grouped.continuous_use_medications
from base
    left join alergias_grouped 
      on alergias_grouped.paciente_cpf = base.cpf
    left join medicamentos_cronicos_grouped
      on medicamentos_cronicos_grouped.paciente_cpf = base.cpf