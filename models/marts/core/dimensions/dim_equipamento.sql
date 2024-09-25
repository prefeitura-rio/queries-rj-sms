{{
    config(
        schema="saude_cnes",
        alias="equipamento_sus_rio_historico"
    )
}}

with
versao_atual as (
    select MAX(data_particao) as versao 
    from {{ ref("raw_cnes_web__tipo_unidade") }}
),

estabelecimentos_mrj_sus as (
    select * from {{ ref("dim_estabelecimento_sus_rio_historico") }} where safe_cast(data_particao as string) = (select versao from versao_atual)
),

equip as (
  select 
    ano,
    mes,
    lpad(id_estabelecimento_cnes, 7, '0') as id_cnes,
    safe_cast(tipo_equipamento as int64) as equipamento_tipo,
    safe_cast(id_equipamento as int64) as equipamento_especifico_tipo,
    safe_cast(quantidade_equipamentos as int64) as equipamentos_quantidade,
    safe_cast(quantidade_equipamentos_ativos as int64) as equipamentos_quantidade_ativos,

  from {{ ref("raw_cnes_ftp__equipamento") }}
  where indicador_equipamento_disponivel_sus = 1 and ano >= 2008 and safe_cast(id_estabelecimento_cnes as int64) in (select distinct safe_cast(id_cnes as int64) from estabelecimentos_mrj_sus)
),

equip_mapping_geral as (
  select
    equipamento_tipo,
    equipamento
  from {{ref ("raw_cnes_web__tipo_equipamento") }}
  where data_particao = (SELECT versao FROM versao_atual)
),

equip_mapping_especifico as (
  select
    equipamento_especifico_tipo,
    equipamento_tipo,
    equipamento_especifico
  from {{ref ("raw_cnes_web__tipo_equipamento_especifico") }}
  where data_particao = (SELECT versao FROM versao_atual)
),

final as (
    select 
        estabs.* except(id_cnes, ano, mes),
        equip.*,
        map_geral.* except(equipamento_tipo),
        map_espec.* except(equipamento_tipo, equipamento_especifico_tipo)

    from equip
    left join estabelecimentos_mrj_sus as estabs using (id_cnes, ano, mes)
    left join equip_mapping_geral as map_geral using (equipamento_tipo)
    left join equip_mapping_especifico as map_espec using (equipamento_tipo, equipamento_especifico_tipo)
)

select * from final