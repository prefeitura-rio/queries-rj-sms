{{
    config(
        enabled=true,
        alias="agendamentos_sisreg",
        materialized="table",
        partition_by={
            "field": "dia_marcacao",
            "data_type": "date",
            "granularity": "day",
        },
    )
}}

WITH

-- ------------------------------------------------------------
-- Telefones cadastrados
-- ------------------------------------------------------------
telefones_cadastros as (
  select 
    cpf as paciente_cpf,
    numero_telefone.ddd as telefone_ddd,
    numero_telefone.valor as telefone_valor,
    numero_telefone.rank as telefone_rank
  from {{ ref('mart_historico_clinico__paciente') }},
    unnest(contato.telefone) as numero_telefone
),

-- ------------------------------------------------------------
-- Telefones do SISREG
-- ------------------------------------------------------------
telefones_sisreg as (
  select 
    paciente_cpf,
    regexp_extract(telefone, r'^\(\d{2}\)', 1) as telefone_ddd,
    regexp_replace(telefone, r'^\(\d{2}\)', '') as telefone_valor,
    0 as telefone_rank
  FROM {{ ref("raw_sisreg_api__marcacoes") }},
    unnest(split(paciente_telefone, ',')) telefone
),
telefones_sisreg_padronizados as (
  select 
    paciente_cpf,
    REGEXP_REPLACE(telefone_ddd, r'[()]', '') AS telefone_ddd,
    {{ padronize_telefone('telefone_valor') }} as telefone_valor,
    telefone_rank
  FROM telefones_sisreg
),

-- ------------------------------------------------------------
-- Unificação e Filtro de telefones
-- ------------------------------------------------------------
todos_telefones as (
  select * from telefones_cadastros
  union all
  select * from telefones_sisreg_padronizados
),

telefones_filtrados as (
  select 
    *
  from todos_telefones
  where
    telefone_ddd = '21' and
    length(telefone_valor) = 9
),

telefones_ordenados as (
  select 
    *
  from telefones_filtrados
  order by telefone_rank asc nulls last
),

-- ------------------------------------------------------------
-- Agrupamentos de telefones
-- ------------------------------------------------------------
telefone_por_cpf as (
  select 
    paciente_cpf,
    array_agg(
      struct(
        telefone_ddd,
        telefone_valor,
        telefone_rank
      )
    ) as telefones
  from telefones_ordenados
  group by 1
)

SELECT
  struct(
    marcacoes.paciente_cpf as cpf,
    marcacoes.paciente_nome as nome,
    telefone_por_cpf.telefones[offset(0)] as telefone
  ) as paciente,

  procedimento_sigtap,
  safe_cast(data_marcacao as date) as dia_marcacao,

  struct(
    unidade_executante_nome as nome,
    unidade_executante_id as cnes,
    unidade_executante_logradouro as logradouro,
    unidade_executante_complemento as complemento,
    unidade_executante_numero as numero,
    unidade_executante_bairro as bairro,
    unidade_executante_municipio as municipio,
    {{ padronize_telefone('unidade_executante_telefone') }} as telefone
  ) as unidade
FROM {{ ref("raw_sisreg_api__marcacoes") }} marcacoes
    left join telefone_por_cpf on telefone_por_cpf.paciente_cpf = marcacoes.paciente_cpf
WHERE 
  solicitacao_status in (
      "SOLICITAÇÃO / AGENDADA / COORDENADOR",
      "SOLICITAÇÃO / AGENDADA / SOLICITANTE",
      "AGENDAMENTO / PENDENTE CONFIRMAÇÃO / EXECUTANTE",
      "AGENDAMENTO / CONFIRMADO / EXECUTANTE"
  ) and
  data_marcacao > current_timestamp() and
  marcacao_executada = "0" and
  unidade_executante_id in (
    SELECT id_cnes
    FROM {{ ref("dim_estabelecimento") }}
  )