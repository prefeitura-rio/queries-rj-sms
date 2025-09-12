{{ config(
    schema="saude_historico_clinico",
    alias = "vinculos_familiares",
    materialized = "table"
) }}

with

-- ------------------------------
-- Validação Cadastral usando RMI
-- ------------------------------
cpf_validado_na_receita_federal as (
  select cpf, upper(nome) as nome, data_nascimento, upper(nome_mae) as nome_mae
  from {{ source("brutos_iplanrio", "registro_municipal_integrado") }}
),

cidadaos_no_pep as (
  select distinct 
    cpf,
    data_nascimento,
    upper(nome) as nome,
    upper(mae_nome) as nome_mae,
    upper(pai_nome) as nome_pai,
    sexo,
    id_cnes
  from {{ ref('raw_prontuario_vitacare__paciente') }}
),

cidadaos_no_pep_avaliados as (
  select 
    cidadaos_no_pep.cpf, 
    cidadaos_no_pep.data_nascimento,
    cidadaos_no_pep.id_cnes,

    cidadaos_no_pep.nome as nome_pep, 
    cpf_validado_na_receita_federal.nome as nome_receita_federal,
    edit_distance(cidadaos_no_pep.nome, cpf_validado_na_receita_federal.nome) as grau_diferenca_nome,

    cidadaos_no_pep.nome_mae as nome_mae_pep,
    cpf_validado_na_receita_federal.nome_mae as nome_mae_receita_federal,
    edit_distance(cidadaos_no_pep.nome_mae, cpf_validado_na_receita_federal.nome_mae) as grau_diferenca_nome_mae,

    cidadaos_no_pep.nome_pai as nome_pai_pep,
    cidadaos_no_pep.sexo as sexo_pep,
  from cidadaos_no_pep
    inner join cpf_validado_na_receita_federal using (cpf, data_nascimento)
),

cidadaos_no_pep_validados as (
  select 
    cpf,
    data_nascimento,
    nome_receita_federal as nome,
    nome_mae_receita_federal as nome_mae,
    nome_pai_pep as nome_pai,
    sexo_pep as sexo,
    id_cnes
  from cidadaos_no_pep_avaliados
  where grau_diferenca_nome < 5
),

-- ------------------------------
-- Vinculação de Pais e Mães
-- ------------------------------
cidacao as (
  select
    cpf,
    nome,
    data_nascimento,
    id_cnes,
    nome_mae,
    sexo,
    case
      when nome_pai like '%SEM INFO%' then null
      when length(nome_pai) < 15 then null
      else nome_pai
    end as nome_pai,
    (
      case
        when nome_pai like '%SEM INFO%' then 0
        when length(nome_pai) < 15 then 0
        when nome_pai is null then 0
        else 1
      end  
    ) as contagem_preenchimento_pais

  from cidadaos_no_pep_validados
),
-- Priorização de campos preenchidos
cidadao_unique as (
  select * 
  from cidacao
  qualify row_number() over(partition by cpf order by contagem_preenchimento_pais desc) = 1
),

-- ------------------------------
-- Publico Alvo
-- ------------------------------
maes as (
  select distinct 
    cpf as crianca_cpf,
    id_cnes,
    nome_mae
  from cidadao_unique
  where {{process_null('nome_mae')}} is not null
),
pais as (
  select distinct
    cpf as crianca_cpf,
    id_cnes,
    nome_pai
  from cidadao_unique
  where {{process_null('nome_pai')}} is not null
),

-- ------------------------------
-- Base de Enriquecimento
-- ------------------------------
base_homens_biologicos as (
  select distinct
    cpf,
    id_cnes,
    nome
  from cidadao_unique
  where sexo in ('MASCULINO', 'MALE')
),
base_mulheres_biologicas as (
  select distinct
    cpf,
    id_cnes,
    nome
  from cidadao_unique
  where sexo in ('FEMININO', 'FEMALE')
),

-- ------------------------------
-- Linkage
-- ------------------------------
pais_linkage_total as (
  select
    pais.*,
    base_homens_biologicos.cpf as pai_candidato_cpf,
    base_homens_biologicos.nome as pai_candidato_nome
  from pais
    inner join base_homens_biologicos on (pais.id_cnes = base_homens_biologicos.id_cnes and pais.nome_pai = base_homens_biologicos.nome)
),
maes_linkage_total as (
  select
    maes.*,
    base_mulheres_biologicas.cpf as mae_candidata_cpf,
    base_mulheres_biologicas.nome as mae_candidata_nome
  from maes
    inner join base_mulheres_biologicas on (maes.id_cnes = base_mulheres_biologicas.id_cnes and maes.nome_mae = base_mulheres_biologicas.nome)
)
select
  cidadao_unique.nome,
  cidadao_unique.cpf,
  cidadao_unique.data_nascimento,

  cidadao_unique.nome_mae,
  maes_linkage_total.mae_candidata_nome,
  maes_linkage_total.mae_candidata_cpf,

  cidadao_unique.nome_pai,
  pais_linkage_total.pai_candidato_nome,
  pais_linkage_total.pai_candidato_cpf
from cidadao_unique
  left join maes_linkage_total on (maes_linkage_total.crianca_cpf = cidadao_unique.cpf )
  left join pais_linkage_total on (pais_linkage_total.crianca_cpf = cidadao_unique.cpf )