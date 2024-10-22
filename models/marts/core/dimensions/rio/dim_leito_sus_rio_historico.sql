{{
    config(
        enabled=true,
        schema="saude_cnes",
        alias="leito_sus_rio_historico",
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

    leitos_mapping_cnesftp as (
        select *
        from
            unnest(
                [
                    struct(1 as tipo_leito, "CIRURGICO" as tipo_leito_descr),
                    struct(2, "CLINICO"),
                    struct(3, "COMPLEMENTAR"),
                    struct(4, "OBSTETRICO"),
                    struct(5, "PEDIATRICO"),
                    struct(6, "OUTROS"),
                    struct(7, "HOSPITAL / DIA")
                ]
            )
    ),

    leitos_mapping_cnesweb as (
        select
            id_leito_especialidade as tipo_especialidade_leito,
            leito_especialidade as tipo_especialidade_leito_descr,
            data_particao
        from {{ ref("raw_cnes_web__leito") }}
        where data_particao = (select versao from versao_atual)
    ),

    leitos_mrj_sus as (
        select

            lt.id_cnes,

            estabs.nome_fantasia as estabelecimento_nome_fantasia,
            estabs.esfera as estabelecimento_esfera,
            estabs.tipo_gestao_descr as estabelecimento_gestao,
            estabs.id_ap as id_estabelecimento_ap,
            estabs.ap as estabelecimento_ap,
            estabs.estabelecimento_sms_indicador,

            lt.tipo_leito,
            lt.tipo_especialidade_leito,
            lt.quantidade_total,
            lt.quantidade_contratado,
            lt.quantidade_sus,

            ftp.tipo_leito_descr,
            web.tipo_especialidade_leito_descr,
            lt.ano_competencia,
            lt.mes_competencia,
            parse_date('%Y-%m-%d', web.data_particao) as data_particao,

        from {{ ref("int_leito_sus_rio_historico__brutos_filtrados") }} as lt
        left join dim_estabelecimentos_sus_rio_historico as estabs using(ano_competencia, mes_competencia, id_cnes)
        left join
            leitos_mapping_cnesftp as ftp
            on safe_cast(lt.tipo_leito as int64) = ftp.tipo_leito
        left join
            leitos_mapping_cnesweb as web
            on safe_cast(lt.tipo_especialidade_leito as int64)
            = safe_cast(web.tipo_especialidade_leito as int64)
    )

select *
from leitos_mrj_sus
