{{
  config(
    alias = "ficha_a_anonimizada",
    materialized = "table",
    meta={"owner": "karen"}
  )
}}

select
  * except (
    id,
    cpf,
    id_paciente_vitacare,
    nome,
    nome_mae,
    nome_pai,
    telefone,
    data_nascimento
  )
from {{ ref('raw_prontuario_vitacare__ficha_a') }}