-- carga horaria ambulatorial dos profissionais no cnes em unidades que oferecem vagas
-- no sisreg
with
    versao_atual as (
        select *
        from {{ ref("mart_cnes_subgeral__profissionais_mrj_sus") }}
        where
            metadado__data_particao = (
                select max(metadado__data_particao)
                from {{ ref("mart_cnes_subgeral__profissionais_mrj_sus") }}
            )
    ),

    profissionais_mais_recentes as (
        select
            -- identificadores
            profissional__cpf as cpf,
            profissional__cns as cns,
            profissional__nome as profissional,
            profissional__id_cbo as id_cbo_2002,
            profissional__cbo as ocupacao,
            profissional__cbo_familia as ocupacao_agg,

            -- carga horaria mensal APROXIMADA
            round(
                profissional__carga_horaria_ambulatorial * 4.5
            ) as carga_horaria_ambulatorial_mensal,

            -- informação temporal
            metadado__ano_competencia as ano_competencia,
            metadado__mes_competencia as mes_competencia,

            -- dados dos estabelecimentos
            estabelecimento__id_cnes as id_cnes,
            estabelecimento__nome_fantasia as estabelecimento,
            estabelecimento__esfera as esfera,
            estabelecimento__natureza_juridica_descr as natureza_juridica,
            estabelecimento__tipo_gestao_descr as tipo_gestao,
            estabelecimento__turno_atendimento as turno,
            estabelecimento__tipo_unidade_alternativo as tipo_unidade_alternativo,
            estabelecimento__tipo_unidade_agrupado as tipo_unidade_agrupado,
            estabelecimento__id_ap as id_ap,
            estabelecimento__ap as ap,
            estabelecimento__endereco_bairro as endereco_bairro,

            row_number() over (
                partition by profissional__cpf
                order by metadado__ano_competencia desc, metadado__mes_competencia desc
            ) as rn

        from versao_atual
        where
            -- selecionando periodo de interesse
            metadado__ano_competencia >= 2020

            -- selecionando apenas estabelecimentos que ja programaram vagas no sisreg
            -- historicamente
            and estabelecimento__id_cnes
            in (select id_cnes from {{ ref("int_mva__oferta_programada_mensal") }})

            -- selecionando apenas profissionais que ja progamaram vagas no sisreg
            -- historicamente
            and profissional__cpf
            in (select cpf from {{ ref("int_mva__oferta_programada_mensal") }})

            -- selecionando apenas profissionais com carga horaria ambulatorial
            and profissional__carga_horaria_ambulatorial > 0

            -- selecionando ocupacoes de interesse
            and regexp_contains(
                profissional__id_cbo,
                r'^(3222|2251|2235|2231|2252|2232|2236|2234|2237|2515|2253|3251|2238|5152|2239)'
            )
            and profissional__id_cbo
            not in ('225142', '225130', '223293', '223565', '322245')  -- exclui saude da familia
    )

select *
from profissionais_mais_recentes
where rn = 1
