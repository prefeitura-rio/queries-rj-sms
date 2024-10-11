{{
    config(
        enabled=true,
        schema="saude_cnes",
        alias="equipamento_sus_rio_historico",
        partition_by = {
            'field': 'data_particao', 
            'data_type': 'date',
            'granularity': 'day'
        }
    )
}}

with
versao_atual as (
    select MAX(data_particao) as versao 
    from {{ ref("raw_cnes_web__tipo_unidade") }}
),

equip as (
  select * from {{ref("int_equipamento_sus_rio_historico__brutos_filtrados")}}
),

equip_mapping_geral as (
  select
    equipamento_tipo,
    equipamento,
    data_particao
  from {{ref ("raw_cnes_web__tipo_equipamento") }}
  where data_particao = (SELECT versao FROM versao_atual)
),

equip_mapping_especifico as (
  select
    equipamento_especifico_tipo,
    equipamento_tipo,
    equipamento_especifico,
  from {{ref ("raw_cnes_web__tipo_equipamento_especifico") }}
  where data_particao = (SELECT versao FROM versao_atual)
),

final as (
  select
      parse_date('%Y-%m-%d', map_geral.data_particao) as data_particao,
      equip.ano_competencia,
      equip.mes_competencia,
      equip.id_cnes,
      equip.equipamento_tipo,
      equipamento,
      equip.equipamento_especifico_tipo,
      equipamento_especifico,
      equipamentos_quantidade,
      equipamentos_quantidade_ativos

  from equip
  left join equip_mapping_geral as map_geral using (equipamento_tipo)
  left join equip_mapping_especifico as map_espec using (equipamento_tipo, equipamento_especifico_tipo)
  order by ano_competencia asc, mes_competencia asc, id_cnes, equipamento_tipo, equipamento_especifico_tipo
)

select * from final