with
versao_atual as (
    select MAX(data_particao) as versao 
    from {{ ref("raw_cnes_web__tipo_unidade") }}
),

estabelecimentos_mrj_sus as (
    select distinct safe_cast(id_cnes as int64) as id_cnes from {{ ref("dim_estabelecimento_sus_rio_historico") }} where safe_cast(data_particao as string) = (select versao from versao_atual)
),

equip_non_unique as (
  select 
    ano as ano_competencia,
    mes as mes_competencia,
    lpad(id_estabelecimento_cnes, 7, '0') as id_cnes,
    safe_cast(tipo_equipamento as int64) as equipamento_tipo,
    safe_cast(id_equipamento as int64) as equipamento_especifico_tipo,
    safe_cast(quantidade_equipamentos as int64) as equipamentos_quantidade,
    safe_cast(quantidade_equipamentos_ativos as int64) as equipamentos_quantidade_ativos,

  from {{ ref("raw_cnes_ftp__equipamento") }}
  where indicador_equipamento_disponivel_sus = 1 and ano >= 2010 and safe_cast(id_estabelecimento_cnes as int64) in (select id_cnes from estabelecimentos_mrj_sus)
)

select distinct * from equip_non_unique