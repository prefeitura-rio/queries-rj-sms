{{
  config(
    schema="exemplo",
    alias="docs_part",
    materialized="table",
    partition_by={
      "field": "data_particao",
      "data_type": "date",
      "granularity": "day",
    },
  )
}}


with
  source as (
    select

      cast(
        cast(rand() * 99999999999 as int64)
        as string
      ) as paciente_cpf,
      cast(
        cast(rand() * 999999999999999 as int64)
        as string
      ) as paciente_cns,

      [
        "João",
        "Maria",
        "José"
      ][offset(
        cast(rand() * 2 as int64)
      )] as paciente_nome,

      date_sub(
        current_date("America/Sao_Paulo"),
        interval
          cast(rand() * 10 as int64)
          day
      ) as paciente_data_nascimento,

      datetime_sub(
        current_datetime("America/Sao_Paulo"),
        interval
          cast(rand() * 10 as int64)
          day
      ) as atendimento_datahora_inicio

    from unnest(generate_array(1, 10000))
  ),

  particao as (
    select
      *,
      date(atendimento_datahora_inicio) as data_particao
    from source
  )

select *
from particao




-- Particionamento pode ser por DIA, MÊS, e ANO, ou NÚMEROS INTEIROS
-- Máximo de partições por tabela são 10,000
-- Máximo de partições por comando (ex.: execução de dbt) são 4,000

-- Qual é a forma mais frequente de busca desses dados?

-- Por data:
-- 4,000 dias ≈ 11 anos
--   Dados históricos do Vitai deram problema com isso
-- 4,000 meses > 300 anos

--   partition_by={
--     "field": "data_particao",
--     "data_type": "date",
--     "granularity": "month",
--   }


-- Por número inteiro, pode ser qualquer coisa:

--   partition_by={
--     "field": "cpf_particao",
--     "data_type": "int64",
--     "range": {"start": 0, "end": 100000000000, "interval": 34722222},
--   }

--   partition_by={
--     "field": "cns_particao",
--     "data_type": "int64",
--     "range": {"start": 0, "end": 1000000000000000, "interval": 333333333334},
--   }


-- FARM_FINGERPRINT() retorna um hash int64 de qualquer string/byte
-- Então FARM_FINGERPRINT(primeiro_nome) = int64 = particionável

--   partition_by={
--     "field": "hash_particao",
--     "data_type": "int64",
--     "range": {"start": 0, "end": 1024, "interval": 1},
--   }
--
--   mod(
--     abs(
--       farm_fingerprint(primeiro_nome)
--     ),
--     1024
--   ) as hash_particao
