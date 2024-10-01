-- carga horaria ambulatorial dos profissionais no cnes em unidades que oferecem vagas no sisreg
-- view

with versao_atual as (
    select *
    from {{ ref('dim_profissional_sus_rio_historico') }}
    where metadados.data_particao = (
        select max(metadados.data_particao)
        from {{ ref('dim_profissional_sus_rio_historico') }}
    )
)

select
    -- identificadores
    profissionais.cpf,
    profissionais.cns,
    profissionais.nome as profissional,
    profissionais.id_cbo as id_cbo_2002,
    upper(profissionais.cbo) as ocupacao,
    upper(profissionais.cbo_familia) as ocupacao_agg,

    -- carga horaria mensal APROXIMADA
    round(sum(profissionais.carga_horaria_ambulatorial * 4.5)) as carga_horaria_ambulatorial_mensal,

    -- informação temporal
    profissionais.ano,
    profissionais.mes,

    -- dados dos estabelecimentos
    estabelecimentos.id_cnes,
    estabelecimentos.nome_fantasia as estabelecimento,
    estabelecimentos.esfera,
    estabelecimentos.natureza_juridica_descr as natureza_juridica,
    estabelecimentos.tipo_gestao_descr as tipo_gestao,
    estabelecimentos.turno_atendimento as turno,
    estabelecimentos.tipo,
    estabelecimentos.tipo_unidade_alternativo,
    estabelecimentos.tipo_unidade_agrupado,
    estabelecimentos.id_ap,
    estabelecimentos.ap,
    estabelecimentos.endereco_bairro

from versao_atual

where
    -- selecionando periodo de interesse
    profissionais.ano >= 2020

    -- selecionando apenas estabelecimentos que oferecem vagas no sisreg
    and estabelecimentos.id_cnes in (select distinct id_cnes from {{ ref('int_mva__oferta_programada_mensal') }})

    -- selecionando apenas estabelecimentos da secretaria municipal de saude
    and estabelecimentos.estabelecimento_sms_indicador = 1

    -- selecionando apenas profissionais com carga horaria ambulatorial
    and profissionais.carga_horaria_ambulatorial > 0

    -- selecionando ocupacoes de interesse
    and regexp_contains(profissionais.id_cbo, r'^(3222|2251|2235|2231|2252|2232|2236|2234|2237|2515|2253|3251|2238|5152|2239)')
    and profissionais.id_cbo not in ('225142', '225130', '223293', '223565', '322245') -- exclui saude da familia

group by
    profissionais.cpf,
    profissionais.cns,
    profissional,
    id_cbo_2002,
    ocupacao,
    ocupacao_agg,
    profissionais.ano,
    profissionais.mes,
    estabelecimentos.id_cnes,
    estabelecimento,
    estabelecimentos.esfera,
    natureza_juridica,
    tipo_gestao,
    turno,
    estabelecimentos.tipo,
    estabelecimentos.tipo_unidade_alternativo,
    estabelecimentos.tipo_unidade_agrupado,
    estabelecimentos.id_ap,
    estabelecimentos.ap,
    estabelecimentos.endereco_bairro