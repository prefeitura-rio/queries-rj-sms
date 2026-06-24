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
  select distinct p.cpf, p.nome, right(v.id_unidade,7) as id_cnes, ep.id_cbo, c.descricao, v.data_particao
  from {{ref('raw_gdb_cnes__vinculo')}} v
    inner join {{ref('raw_gdb_cnes__profissional')}} p using (id_profissional_sus)
    inner join {{ref('raw_gdb_cnes__equipe_profissionais')}} ep using (id_profissional_sus)
    inner join {{ref('raw_gdb_cnes__equipe')}} e on e.equipe_sequencial = ep.equipe_sequencial
    inner join {{ref('raw_datasus__cbo')}} c on (ep.id_cbo = c.id_cbo)
    where
      v.data_particao = (select data_particao from ultima_particao_disponivel) and (
        regexp_contains(lower(c.descricao), 'enfermei') or
        regexp_contains(lower(c.descricao), '^medic') or
        regexp_contains(lower(c.descricao), 'agente com')
      ) and (
        not regexp_contains(lower(c.descricao), 'socorrista') and
        not regexp_contains(lower(c.descricao), 'ecnico') and
        not regexp_contains(lower(c.descricao), 'sanitarista')
      )
),

vinculos_atencao_primaria as (
  select v.*
  from vinculos v
    inner join {{ref('dim_estabelecimento')}} e using (id_cnes)
  where e.tipo_sms_agrupado = 'APS'
)

select *
from vinculos_atencao_primaria