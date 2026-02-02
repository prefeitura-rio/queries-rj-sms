{{
    config(
        schema="intermediario_historico_clinico",
        alias="contrarreferencia_sarah",
        materialized="table",
        unique_key=['id_hci'],
        cluster_by=['id_hci'],
        partition_by={
            "field": "data_particao",
            "data_type": "date",
            "granularity": "day",
        },
    )
}}

with source as (
  select *
  from {{ ref("raw_prontuario_sarah__contrarreferencia") }}
),

  municipios as (
    select
      nome_uf,
      cod_mun,
      nome_mun as nome_municipio
    from {{ ref("raw_sheets__municipios_brasil") }}
  ),

  cidades_particionado as (
    select

      {{
        dbt_utils.generate_surrogate_key(
          [
            "s.source_id",
            "s.paciente_cpf",
            "s.contrarreferencia_datahora",
          ]
        )
      }} as id_hci,

      s.* except (
        unidade_cod_municipio,
        paciente_cod_municipio_naturalidade,
        paciente_cod_municipio_residencia
      ),
      m1.nome_uf as unidade_uf,
      s.unidade_cod_municipio,
      m1.nome_municipio as unidade_municipio,

      m2.nome_uf as paciente_uf_naturalidade,
      s.paciente_cod_municipio_naturalidade,
      m2.nome_municipio as paciente_municipio_naturalidade,

      m2.nome_uf as paciente_uf_residencia,
      s.paciente_cod_municipio_residencia,
      m3.nome_municipio as paciente_municipio_residencia,

      safe_cast(contrarreferencia_datahora as date) as data_particao
    from source as s
    -- Se alguém souber uma forma mais elegante do que 3 joins, sou todo ouvidos
    left join municipios as m1
      on m1.cod_mun = s.unidade_cod_municipio
    left join municipios as m2
      on m2.cod_mun = s.paciente_cod_municipio_naturalidade
    left join municipios as m3
      on m3.cod_mun = s.paciente_cod_municipio_residencia
  )

select *
from cidades_particionado
