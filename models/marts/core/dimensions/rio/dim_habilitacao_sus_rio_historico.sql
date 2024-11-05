{{
    config(
        enabled=true,
        schema="saude_cnes",
        alias="habilitacao_sus_rio_historico",
        partition_by={
            "field": "data_particao",
            "data_type": "date",
            "granularity": "day",
        },
    )
}}

with
    versao_atual as (
        select max(data_particao) as versao from {{ ref("raw_cnes_web__tipo_unidade") }}
    ),

    dim_estabelecimentos_sus_rio_historico as (
      select * from {{ref("dim_estabelecimento_sus_rio_historico")}} where safe_cast(data_particao as string) = (select versao from versao_atual)  
    ),

    habilitacoes_mapping_cnesweb as (
        select id_habilitacao, habilitacao, tipo_origem, tipo_habilitacao, data_particao
        from {{ ref("raw_cnes_web__tipo_habilitacao") }}
        where
            data_particao = (select versao from versao_atual)

            -- removendo ids ambiguos (não unicos).. são poucos
            and id_habilitacao not in (
                select id_habilitacao
                from
                    (
                        select id_habilitacao, count(*) as contagem
                        from {{ ref("raw_cnes_web__tipo_habilitacao") }}
                        where data_particao = (select versao from versao_atual)
                        group by id_habilitacao
                        having contagem > 1
                    )
            )
    ),

    habilitacoes as (
        select
            hab.id_cnes,

            estabs.nome_fantasia as estabelecimento_nome_fantasia,
            estabs.esfera as estabelecimento_esfera,
            estabs.tipo_gestao_descr as estabelecimento_gestao,
            estabs.id_ap as id_estabelecimento_ap,
            estabs.ap as estabelecimento_ap,
            estabs.estabelecimento_sms_indicador,

            hab.id_habilitacao,
            habilitacao,
            habilitacao_ativa_indicador,
            nivel_habilitacao,
            tipo_origem,
            habilitacao_ano_inicio,
            habilitacao_mes_inicio,
            habilitacao_ano_fim,
            habilitacao_mes_fim,
            ano_competencia,
            mes_competencia,
            parse_date('%Y-%m-%d', map.data_particao) as data_particao,

        from {{ ref("int_habilitacao_sus_rio_historico__brutos_filtrados") }} as hab
        left join dim_estabelecimentos_sus_rio_historico as estabs using(ano_competencia, mes_competencia, id_cnes)
        left join
            habilitacoes_mapping_cnesweb as map
            on safe_cast(hab.id_habilitacao as int64)
            = safe_cast(map.id_habilitacao as int64)
    )

select *
from habilitacoes