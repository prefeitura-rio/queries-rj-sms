{{ config(
    schema = "intermediario_whatsapp",
    alias = "vinculos_recem_nascidos",
    materialized = "table"
) }}

with

-- Recem Nascidos
recem_nascidos as (
  select
    cpf,
    nome,
    id_cnes,
    case
      when mae_nome like '%SEM INFO%' then null
      when length(mae_nome) < 15 then null
      else mae_nome
    end as mae_nome,
    case
      when pai_nome like '%SEM INFO%' then null
      when length(pai_nome) < 15 then null
      else pai_nome
    end as pai_nome,
    (
      case
        when mae_nome like '%SEM INFO%' then 0
        when length(mae_nome) < 15 then 0
        when mae_nome is null then 0
        else 1
      end
      +
      case
        when pai_nome like '%SEM INFO%' then 0
        when length(pai_nome) < 15 then 0
        when pai_nome is null then 0
        else 1
      end  
    ) as contagem_preenchimento_pais

  from {{ ref('raw_prontuario_vitacare__paciente') }}
  where data_nascimento >= '2025-05-01' and obito_indicador = false
),
-- Priorização de campos preenchidos
recem_nascidos_unique as (
  select * 
  from
  recem_nascidos
  qualify row_number() over(partition by cpf order by contagem_preenchimento_pais desc) = 1

),
-- Mães a serem identificadas por CPF
maes as (
  select distinct 
    cpf as crianca_cpf,
    id_cnes,
    mae_nome,
    SUBSTR(mae_nome, 0,1) as letra
  from recem_nascidos_unique
  where {{process_null('mae_nome')}} is not null
),

-- Pais a serem identificadas por CPF
pais as (
  select distinct
    cpf as crianca_cpf,
    id_cnes,
    pai_nome,
    SUBSTR(pai_nome, 0,1) as letra
  from recem_nascidos
  where {{process_null('pai_nome')}} is not null
),

-- ------------------------------
-- LINKAGE
-- ------------------------------
base_homens_biologicos as (
  select distinct
    cpf,
    id_cnes,
    nome,
    SUBSTR(nome, 0,1) as letra
  from {{ ref('raw_prontuario_vitacare__paciente') }}
  where sexo = 'MASCULINO'
),
base_mulheres_biologicas as (
  select distinct
    cpf,
    id_cnes,
    nome,
    SUBSTR(nome, 0,1) as letra
  from {{ ref('raw_prontuario_vitacare__paciente') }}
  where sexo = 'FEMININO'
),

-- Processo de Linkage: diminuindo espaço usando primeira letra do nome e cnes do cadastro
pais_linkage_total as (
  select
    pais.*,
    base_homens_biologicos.cpf as pai_candidato_cpf,
    base_homens_biologicos.nome as pai_candidato_nome,
    edit_distance(
      REGEXP_REPLACE(lower(pais.pai_nome),'( de)|( da)|( do)',''), 
      REGEXP_REPLACE(lower(base_homens_biologicos.nome),'( de)|( da)|( do)','')
      ) as distance
  from pais
    inner join base_homens_biologicos using (id_cnes, letra)
),
maes_linkage_total as (
  select
    maes.*,
    base_mulheres_biologicas.cpf as mae_candidata_cpf,
    base_mulheres_biologicas.nome as mae_candidata_nome,
    edit_distance(
      REGEXP_REPLACE(lower(maes.mae_nome),'( de)|( da)|( do)',''), 
      REGEXP_REPLACE(lower(base_mulheres_biologicas.nome),'( de)|( da)|( do)','')
      ) as distance
  from maes
    inner join base_mulheres_biologicas using (id_cnes, letra)
),

-- Ranqueamento do linkage com menor distancia, distance < 2
pais_linkage_melhor as (
  select *
  from pais_linkage_total
  where distance < 2
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY crianca_cpf
    ORDER BY distance ASC
  ) = 1
),
maes_linkage_melhor as (
  select *
  from maes_linkage_total
  where distance < 2
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY crianca_cpf
    ORDER BY distance ASC
  ) = 1
)
select
  recem_nascidos_unique.nome,
  recem_nascidos_unique.cpf,

  recem_nascidos_unique.mae_nome,
  maes_linkage_melhor.mae_candidata_nome,
  maes_linkage_melhor.mae_candidata_cpf,

  recem_nascidos_unique.pai_nome,
  pais_linkage_melhor.pai_candidato_nome,
  pais_linkage_melhor.pai_candidato_cpf
from recem_nascidos_unique
  left join maes_linkage_melhor on (maes_linkage_melhor.crianca_cpf = recem_nascidos_unique.cpf )
  left join pais_linkage_melhor on (pais_linkage_melhor.crianca_cpf = recem_nascidos_unique.cpf )