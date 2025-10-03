{{
  config(
    materialized="table",
    schema="processo_seletivo_dit",
    alias = "ficha_a_anonimizada"
  )
}}

select
  * except (
    id,
    cpf,
    numero_prontuario,
    id_paciente_vitacare,
    nome,
    nome_social,
    nome_mae,
    nome_pai,
    telefone,
    data_nascimento,
    logradouro,
    tipo_logradouro,
    microarea,
    tipo,
    updated_at,
    loaded_at
  )
from {{ ref('raw_prontuario_vitacare__ficha_a') }}