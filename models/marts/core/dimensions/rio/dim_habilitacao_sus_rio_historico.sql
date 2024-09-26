{{
    config(
        schema="saude_cnes",
        alias="habilitacao_sus_rio_historico"
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

habilitacoes as (
  select
    ano,
    mes,
    id_estabelecimento_cnes as id_cnes,
    tipo_habilitacao as id_habilitacao,
    nivel_habilitacao,
    case
      when ano_competencia_final = 9999 then 1
      else 0
    end as habilitacao_ativa_indicador,
    ano_competencia_inicial as habilitacao_ano_inicio,
    mes_competencia_inicial as habilitacao_mes_inicio,
    case
      when ano_competencia_final = 9999 then NULL
      else ano_competencia_final
    end as habilitacao_ano_fim,
    case
      when mes_competencia_final = 99 then NULL
      else mes_competencia_final
    end as habilitacao_mes_fim
  from
    {{ref("raw_cnes_ftp__habilitacao")}}
  where ano >= 2008 and safe_cast(id_estabelecimento_cnes as int64) in (select distinct safe_cast(id_cnes as int64) from estabelecimentos_mrj_sus)
),

habilitacoes_mapping_cnesweb as (
  select
    id_habilitacao,
    habilitacao,
    tipo_origem,
    tipo_habilitacao

  from {{ ref("raw_cnes_web__tipo_habilitacao") }}
  where data_particao = (SELECT versao FROM versao_atual)
),

final as (
    select
        estabs.* except (ano, mes, id_cnes),
        hab.*,
        map.* except (id_habilitacao),
    from habilitacoes as hab
    left join habilitacoes_mapping_cnesweb as map on safe_cast(hab.id_habilitacao as int64) = safe_cast(map.id_habilitacao as int64)
    left join estabelecimentos_mrj_sus as estabs on (hab.ano = estabs.ano and hab.mes = estabs.mes and safe_cast(hab.id_cnes as int64) = safe_cast(estabs.id_cnes as int64))
)

select * from final