-- carga horaria ambulatorial dos profissionais no cnes em unidades que oferecem vagas
-- no sisreg
-- view
with
    versao_atual as (
        select *
        from {{ ref("mart_cnes_subgeral__profissionais_mrj_sus") }}
        where
            metadado__data_particao = (
                select max(metadado__data_particao)
                from {{ ref("mart_cnes_subgeral__profissionais_mrj_sus") }}
            )
    )

select
    -- identificadores
    profissional__cpf as cpf,
    array_agg(distinct profissional__cns ignore nulls) as cns,
    array_agg(distinct profissional__nome ignore nulls) as profissional,
    array_agg(distinct profissional__id_cbo ignore nulls) as id_cbo_2002,
    array_agg(distinct profissional__cbo ignore nulls) as ocupacao,
    array_agg(distinct profissional__cbo_familia ignore nulls) as ocupacao_agg,

    -- carga horaria mensal APROXIMADA
    round(
        sum(profissional__carga_horaria_ambulatorial * 4.5)
    ) as carga_horaria_ambulatorial_mensal,

    -- informação temporal
    metadado__ano_competencia as ano_competencia,
    metadado__mes_competencia as mes_competencia,

    -- dados dos estabelecimentos
    estabelecimento__id_cnes as id_cnes,
    array_agg(distinct estabelecimento__nome_fantasia ignore nulls) as estabelecimento,
    array_agg(distinct estabelecimento__esfera ignore nulls) as esfera,
    array_agg(
        distinct estabelecimento__natureza_juridica_descr ignore nulls
    ) as natureza_juridica,
    array_agg(distinct estabelecimento__tipo_gestao_descr ignore nulls) as tipo_gestao,
    array_agg(distinct estabelecimento__turno_atendimento ignore nulls) as turno,
    array_agg(
        distinct estabelecimento__tipo_unidade_alternativo ignore nulls
    ) as tipo_unidade_alternativo,
    array_agg(
        distinct estabelecimento__tipo_unidade_agrupado ignore nulls
    ) as tipo_unidade_agrupado,
    array_agg(distinct estabelecimento__id_ap ignore nulls) as id_ap,
    array_agg(distinct estabelecimento__ap ignore nulls) as ap,
    array_agg(
        distinct estabelecimento__endereco_bairro ignore nulls
    ) as endereco_bairro,

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
    and profissional__id_cbo not in ('225142', '225130', '223293', '223565', '322245')  -- exclui saude da familia

group by cpf, id_cnes, ano_competencia, mes_competencia
