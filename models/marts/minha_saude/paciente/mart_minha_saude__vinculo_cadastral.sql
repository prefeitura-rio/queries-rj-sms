{{
  config(
    alias="vinculo_cadastral",
    materialized="table",
    partition_by={
      "field": "cpf_particao",
      "data_type": "int64",
      "range": {"start": 0, "end": 100000000000, "interval": 34722222},
    },
  )
}}

with
  source as (
    select distinct

      cpf_paciente,
      cns_paciente,
      nome_paciente,
      data_nascimento,

      id_cnes,
      nome_unidade,
      cadastro_permanente_indicador,

      id_ine,
      nome_equipe_familia,
      data_atualizacao_vinculo_equipe,

      data_ultima_atualizacao_cadastral,
      safe_cast(cpf_paciente as int64) as cpf_particao

    from {{ ref("int_historico_clinico__vinculo_equipe_familia") }}
  )

select *
from source
