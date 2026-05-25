{{
    config(
        schema="intermediario_prontuario_vitacare",
        alias="paciente",
        materialized="table",
        tags=['daily']
    )
}}

select *
from {{ ref('raw_prontuario_vitacare__paciente') }}
where cpf is not null
  and id_cnes is not null