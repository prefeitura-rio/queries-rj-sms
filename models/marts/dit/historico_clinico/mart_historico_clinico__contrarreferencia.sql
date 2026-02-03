{{
    config(
        schema="saude_historico_clinico",
        alias="contrarreferencia",
        materialized="table",
        partition_by={
            "field": "cpf_particao",
            "data_type": "int64",
            "range": {"start": 0, "end": 100000000000, "interval": 34722222},
        },
    )
}}

with source as (
  select

    source_id,
    id_hci,

    struct(
      paciente_nome as nome,
      paciente_nome_social as nome_social,
      paciente_cpf as cpf,
      paciente_cns as cns,
      paciente_data_nascimento as data_nascimento,
      paciente_telefone as telefone,

      paciente_uf_naturalidade as uf_naturalidade,
      paciente_municipio_naturalidade as municipio_naturalidade,
      paciente_uf_residencia as uf_residencia,
      paciente_municipio_residencia as municipio_residencia
    ) as paciente,

    struct(
      id_cnes as id_cnes,
      unidade_nome as nome,
      unidade_uf as uf,
      unidade_municipio as municipio
    ) as estabelecimento,

    struct(
      profissional_nome as nome,
      profissional_cpf as cpf,
      profissional_cns as cns,
      profissional_cbo as cbo  -- TODO: cruzar com CBO
    ) as profissional,

    struct(
      id_documento,
      contrarreferencia_numero as numero,
      safe_cast(contrarreferencia_datahora as datetime) as datahora
    ) as contrarreferencia,

    struct(
      case
        when motivo = "Não foi registrada."
          then null
        else motivo
      end as motivo,
      impressao,
      resultados,
      conduta,
      case
        when lower(trim(conduta_seguimento)) = lower(trim(conduta))
          then null
        when REGEXP_CONTAINS(
          lower(trim(conduta_seguimento)),
          r"(?i)^(as\s*)?acima$"
        )
          then null
        else trim(conduta_seguimento)
      end as seguimento,
      resumo,
      encaminhamento
    ) as avaliacao,

    struct(
      cid_principal as cid,
      diagnostico_principal as descricao
    ) as diagnostico,

    flag_problema,
    flag_motivo_coincide,

    data_particao,
    safe_cast(paciente_cpf as int64) as cpf_particao

  from {{ ref("int_historico_clinico__contrarreferencia") }}
)

select *
from source
