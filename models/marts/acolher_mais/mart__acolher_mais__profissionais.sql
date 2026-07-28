{{
    config(
        alias="profissionais",
        materialized="table"
    )
}}

with

ultima_particao_disponivel as (
  select max(data_particao) as data_particao
  from {{ref('raw_gdb_cnes__vinculo')}}
),

profissionais as (
  select *
  from {{ref('raw_gdb_cnes__profissional')}}
  where data_particao = (select data_particao from ultima_particao_disponivel)
),

equipes as (
  select *
  from {{ref('raw_gdb_cnes__equipe')}}
  where data_particao = (select data_particao from ultima_particao_disponivel)
),

equipe_profissionais as (
  select *
  from {{ref('raw_gdb_cnes__equipe_profissionais')}}
  where data_particao = (select data_particao from ultima_particao_disponivel)
),

vinculos as (
  select p.cpf, p.nome, right(v.id_unidade,7) as id_cnes, ep.id_cbo as ocupacao_cbo, e.equipe_ine, e.equipe_nome
  from {{ref('raw_gdb_cnes__vinculo')}} v
    inner join profissionais p using (id_profissional_sus)
    inner join equipe_profissionais ep using (id_profissional_sus)
    inner join equipes e on e.equipe_sequencial = ep.equipe_sequencial
    left join {{ref('raw_datasus__cbo')}} c on (ep.id_cbo = c.id_cbo)
    where
      v.data_particao = (select data_particao from ultima_particao_disponivel) and (
      ep.id_cbo in (
          '515105', -- ACS
          '322255', -- Tecnico ACS
          '322245', -- Tecnico de Enfermagem da ESF
          '322205', -- Tecnico de Enfermagem
          '223565', -- Enfermeiro
          '223505',  -- Enfermeiro
          '225142', -- Medico da estrategia de saude da familia
          '225124', -- Medico Pediatra
          '225130', -- Medico de Fam e Comunidade
          '223116'  -- Medico de Saude da familia
        )
      ) and
      ep.data_desligamento_profissional is null and e.data_desativacao is null and 
      e.id_equipe_tipo in (
        '70', -- eSF: Equipe de Saúde da Família
        '74', -- eABP: Equipe de Atenção Primária Prisional
        '76' -- eAP: Equipe de Atenção Primária
      )
),

vinculos_atencao_primaria as (
  select v.*
  from vinculos v
    inner join {{ref('dim_estabelecimento')}} e using (id_cnes)
  where e.tipo_sms_agrupado = 'APS'
),

ine_nome_equipe as (
  select 
    equipe_ine,
    equipe_nome
  from vinculos_atencao_primaria
  qualify row_number() over (
    partition by equipe_ine
    order by length(equipe_nome) desc
  ) = 1
),

nome_cpf as (
  select 
    cpf,
    nome
  from vinculos_atencao_primaria
  qualify row_number() over (
    partition by cpf
    order by length(nome) desc
  ) = 1
),

padronizando_vinculos as (
  select
    distinct
    cpf, id_cnes,
    case 
      when ocupacao_cbo = '515105' then 'ACS'
      when ocupacao_cbo = '322255' then 'ACS'
      when ocupacao_cbo = '322245' then 'ENFERMAGEM'
      when ocupacao_cbo = '322205' then 'ENFERMAGEM'
      when ocupacao_cbo = '223565' then 'ENFERMAGEM'
      when ocupacao_cbo = '223505' then 'ENFERMAGEM'
      when ocupacao_cbo = '225142' then 'MEDICO'
      when ocupacao_cbo = '225124' then 'MEDICO'
      when ocupacao_cbo = '225130' then 'MEDICO'
      when ocupacao_cbo = '223116' then 'MEDICO'
    end as ocupacao_descricao,
    equipe_ine
  from vinculos_atencao_primaria
),

sem_duplicados as (
  select 
    cpf, 
    nome_cpf.nome as nome,
    id_cnes, 
    ocupacao_descricao, 
    equipe_ine,
    ine_nome_equipe.equipe_nome as equipe_nome
  from padronizando_vinculos
    inner join ine_nome_equipe using(equipe_ine)
    inner join nome_cpf using(cpf)
),

agrupado_por_pessoa as (
  select 
    cpf, 
    nome_cpf.nome, 
    array_agg(
      struct(id_cnes, ocupacao_descricao, equipe_ine, equipe_nome)
    ) as vinculos
  from sem_duplicados
    left join nome_cpf using(cpf)
  group by 1, 2
)

select *
from agrupado_por_pessoa