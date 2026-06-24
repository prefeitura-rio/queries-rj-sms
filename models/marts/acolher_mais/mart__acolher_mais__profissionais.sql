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

vinculos as (
  select p.cpf, p.nome, right(v.id_unidade,7) as id_cnes, ep.id_cbo as ocupacao_cbo
  from {{ref('raw_gdb_cnes__vinculo')}} v
    inner join {{ref('raw_gdb_cnes__profissional')}} p using (id_profissional_sus)
    inner join {{ref('raw_gdb_cnes__equipe_profissionais')}} ep using (id_profissional_sus)
    inner join {{ref('raw_gdb_cnes__equipe')}} e on e.equipe_sequencial = ep.equipe_sequencial
    inner join {{ref('raw_datasus__cbo')}} c on (ep.id_cbo = c.id_cbo)
    where
      v.data_particao = (select data_particao from ultima_particao_disponivel) and (
      ep.id_cbo in (
          '515105', -- ACS
          '322255', -- Tecnico ACS
          '322245', -- Tecnico de Enfermagem da ESF
          '322205', -- Tecnico de Enfermagem
          '223565', -- Enfermeiro
          '223505'  -- Enfermeiro
          '225142', -- Medico da estrategia de saude da familia
          '225124', -- Medico Pediatra
          '225130', -- Medico de Fam e Comunidade
          '223116'  -- Medico de Saude da familia
        )
      )
),

vinculos_atencao_primaria as (
  select v.*
  from vinculos v
    inner join {{ref('dim_estabelecimento')}} e using (id_cnes)
  where e.tipo_sms_agrupado = 'APS'
),

padronizando_vinculos as (
  select 
    cpf, nome, id_cnes,
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
    end as ocupacao_descricao
  from vinculos_atencao_primaria
),

sem_duplicados as (
  select distinct *
  from padronizando_vinculos
),

agrupado_por_pessoa as (
  select 
    cpf, 
    max(nome) as nome, 
    array_agg(
      struct(id_cnes, ocupacao_descricao)
    ) as vinculos
  from sem_duplicados
  group by 1
)

select *
from agrupado_por_pessoa