with 
versao_atual as (
    select max(data_particao) as versao 
    from {{ ref("raw_cnes_web__tipo_unidade") }}
),

estabelecimentos_mrj_sus as (
    select distinct safe_cast(id_cnes as int64) as id_cnes from {{ ref("dim_estabelecimento_sus_rio_historico") }} where safe_cast(data_particao as string) = (select versao from versao_atual)
),

habilitacoes_non_unique as (
  select
    ano as ano_competencia,
    mes as mes_competencia,
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

  from {{ref("raw_cnes_ftp__habilitacao")}}
  where ano >= 2010 and safe_cast(id_estabelecimento_cnes as int64) in (select id_cnes from estabelecimentos_mrj_sus)
)

select distinct * from habilitacoes_non_unique