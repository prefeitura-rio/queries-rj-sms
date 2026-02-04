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

  cbo as (
    select * 
    from {{ ref("raw_datasus__cbo") }}
  ),

  estabelecimentos as (
    select
      id_cnes,
      {{ proper_estabelecimento("nome_acentuado") }} as nome
    from {{ ref("dim_estabelecimento") }}
  ),

  cbo_cidades_particionado as (
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
        conduta,
        conduta_seguimento,
        unidade_cod_municipio,
        unidade_nome,
        paciente_cod_municipio_naturalidade,
        paciente_cod_municipio_residencia
      ),

      case
        when REGEXP_CONTAINS(
          conduta,
          r"^[\-\.\s;,/=]+$"
        )
          then null
        when REGEXP_CONTAINS(
          lower(conduta),
          r"^(as|vide)?\s*acim\s*a\.?$"
        ) or lower(conduta) = "supracitada"
          then null
        when lower(conduta) = "nenhuma"
          then null
        else REGEXP_REPLACE(
          REGEXP_REPLACE(
            conduta,
            r"^[\-\.\s;,/=]+",
            ""
          ),
          r"[\-\.\s;,/=]+$",
          ""
        )
      end as conduta,

      case
        when lower(conduta_seguimento) = lower(conduta)
          then null
        when REGEXP_CONTAINS(
          conduta_seguimento,
          r"^[\-\.\s;,/=]+$"
        )
          then null
        when REGEXP_CONTAINS(
          lower(conduta_seguimento),
          r"^(as|vide)?\s*acim\s*a\.?$"
        ) or lower(conduta_seguimento) = "supracitada"
          then null
        else REGEXP_REPLACE(
          REGEXP_REPLACE(
            conduta_seguimento,
            r"^[\-\.\s;,/=]+",
            ""
          ),
          r"[\-\.\s;,/=]+$",
          ""
        )
      end as conduta_seguimento,

      cbo.descricao as profissional_cargo,
      coalesce(
        -- Nome de estabelecimento acentuado, se existir na tabela de CNES
        e.nome,
        -- Nome recebido pelo prontuário; às vezes é "Residência", sem CNES
        {{ proper_estabelecimento("unidade_nome") }}
      ) as unidade_nome,

      m1.nome_uf as unidade_uf,
      s.unidade_cod_municipio,
      m1.nome_municipio as unidade_municipio,

      m2.nome_uf as paciente_uf_naturalidade,
      s.paciente_cod_municipio_naturalidade,
      m2.nome_municipio as paciente_municipio_naturalidade,

      m3.nome_uf as paciente_uf_residencia,
      s.paciente_cod_municipio_residencia,
      m3.nome_municipio as paciente_municipio_residencia,

      safe_cast(contrarreferencia_datahora as date) as data_particao
    from source as s
    left join cbo
      on cbo.id_cbo = s.profissional_cbo
    left join estabelecimentos as e
      on e.id_cnes = s.id_cnes
    -- Se alguém souber uma forma mais elegante do que 3 joins, sou todo ouvidos
    left join municipios as m1
      on m1.cod_mun = s.unidade_cod_municipio
    left join municipios as m2
      on m2.cod_mun = s.paciente_cod_municipio_naturalidade
    left join municipios as m3
      on m3.cod_mun = s.paciente_cod_municipio_residencia
  )

select *
from cbo_cidades_particionado
