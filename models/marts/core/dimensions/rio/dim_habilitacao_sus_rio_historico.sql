{{
    config(
        enabled=true,
        schema="saude_cnes",
        alias="habilitacao_sus_rio_historico",
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

habilitacoes_mapping_cnesweb AS (
  SELECT
    id_habilitacao,
    habilitacao,
    tipo_origem,
    tipo_habilitacao,
    data_particao
  FROM 
    {{ ref("raw_cnes_web__tipo_habilitacao") }}
  WHERE
    data_particao = (SELECT versao FROM versao_atual)

    -- removendo ids ambiguos (não unicos).. são poucos
    AND id_habilitacao NOT IN (
      SELECT
        id_habilitacao
      FROM (
        SELECT
          id_habilitacao,
          COUNT(*) AS contagem
        FROM
          {{ ref("raw_cnes_web__tipo_habilitacao") }}
        WHERE
          data_particao = (SELECT versao FROM versao_atual)
        GROUP BY
          id_habilitacao
        HAVING
          contagem > 1
      )
    )
),

habilitacoes as (
  select
        parse_date('%Y-%m-%d', data_particao) as data_particao,
        ano_competencia,
        mes_competencia,
        hab.id_cnes,
        hab.id_habilitacao,
        habilitacao,
        habilitacao_ativa_indicador,
        nivel_habilitacao,
        tipo_origem,
        habilitacao_ano_inicio,
        habilitacao_mes_inicio,
        habilitacao_ano_fim,
        habilitacao_mes_fim

  from {{ref("int_habilitacao_sus_rio_historico__brutos_filtrados")}} as hab
  left join habilitacoes_mapping_cnesweb as map on safe_cast(hab.id_habilitacao as int64) = safe_cast(map.id_habilitacao as int64)
  order by ano_competencia asc, mes_competencia asc, id_cnes asc, habilitacao_ativa_indicador, id_habilitacao
)

select * from habilitacoes