with
versao_atual_sisreg as (select max(data_particao) as versao from  {{ ref("fct_sisreg_oferta_programada_serie_historica") }}),

oferta_programada_mensal_por_procedimento_cbo as (
    select 
        profissional_executante_cpf as cpf,
        id_estabelecimento_executante as id_cnes,
        id_procedimento_interno as id_procedimento,
        id_cbo2002 as id_cbo_2002,
        procedimento_vigencia_ano as ano,
        procedimento_vigencia_mes as mes,

        sum((vagas_primeira_vez_qtd + vagas_reserva_qtd)) as vagas_programadas_mensal_primeira_vez,
        sum(vagas_retorno_qtd) as vagas_programadas_mensal_retorno,
        sum(vagas_todas_qtd) as vagas_programadas_mensal_todas

    from {{ref("fct_sisreg_oferta_programada_serie_historica")}}

    where
        -- selecionando versao mais atual
        data_particao = (select versao from versao_atual_sisreg)

        -- selecionando ocupações de interesse
        and regexp_contains(id_cbo2002, r'^(3222|2251|2235|2231|2252|2232|2236|2234|2237|2515|2253|3251|2238|5152|2239)')
        and id_cbo2002 not in ("225142", "225130", "223293", "223565", "322245") -- excluindo saude da familia

    group by
        cpf,
        id_cnes,
        id_procedimento,
        id_cbo_2002,
        ano,
        mes
),

oferta_programada_mensal_por_unidade as (
    select
        cpf,
        id_cnes,
        ano,
        mes,
        sum(vagas_programadas_mensal_primeira_vez) as vagas_programadas_mensal_primeira_vez_unidade,
        sum(vagas_programadas_mensal_retorno) as vagas_programadas_mensal_retorno_unidade,
        sum(vagas_programadas_mensal_todas) as vagas_programadas_mensal_todas_unidade
    
    from oferta_programada_mensal_por_procedimento_cbo

    group by
        cpf,
        id_cnes,
        ano,
        mes
),

oferta_programada_mensal as (
    select
        -- id
        por_proced_cbo.cpf,
        por_proced_cbo.id_cnes,
        por_proced_cbo.id_procedimento,
        por_proced_cbo.id_cbo_2002,
        por_proced_cbo.ano,
        por_proced_cbo.mes,

        -- primeira vez
        por_proced_cbo.vagas_programadas_mensal_primeira_vez,
        por_unidade.vagas_programadas_mensal_primeira_vez_unidade,

        -- retorno
        por_proced_cbo.vagas_programadas_mensal_retorno,
        por_unidade.vagas_programadas_mensal_retorno_unidade,

        -- total
        por_proced_cbo.vagas_programadas_mensal_todas,
        por_unidade.vagas_programadas_mensal_todas_unidade,

        -- distribuicao de vagas por procedimento (definida pelo próprio profissional)
        case 
            when por_unidade.vagas_programadas_mensal_todas_unidade = 0 then 0
            else por_proced_cbo.vagas_programadas_mensal_todas / por_unidade.vagas_programadas_mensal_todas_unidade
        end as procedimento_distribuicao

    from oferta_programada_mensal_por_procedimento_cbo as por_proced_cbo
    left join oferta_programada_mensal_por_unidade as por_unidade
    using (cpf, id_cnes, ano, mes)
),

profissionais as (
    select
        profissionais.cpf as cpf,
        profissionais.profissional_cns as cns,
        profissionais.profissional_nome as profissional,
        profissionais.id_cbo as id_cbo_2002,
        upper(profissionais.cbo) as ocupacao,
        upper(profissionais.cbo_familia) as ocupacao_agg,
        (profissionais.carga_horaria_ambulatorial * 4.5) as carga_horaria_ambulatorial_mensal,

        estabelecimentos.id_cnes,
        estabelecimentos.ano,
        estabelecimentos.mes,
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

    from {{ref("dim_profissional_sus_rio_historico")}}
    where
        -- selecionando os registros mais atuais
        metadados.data_particao = (select max(metadados.data_particao) from {{ ref("dim_profissional_sus_rio_historico") }})

        -- filtrando por estabelecimentos que possuem oferta no sisreg
        and estabelecimentos.id_cnes in (select distinct id_cnes from oferta_programada_mensal)

        -- selecionando profissionais de interesse
        and estabelecimentos.estabelecimento_sms_indicador = 1
        and profissionais.carga_horaria_ambulatorial > 0
        and regexp_contains(profissionais.id_cbo, r'^(3222|2251|2235|2231|2252|2232|2236|2234|2237|2515|2253|3251|2238|5152|2239)')
        and profissionais.id_cbo not in ("225142", "225130", "223293", "223565", "322245")
)   

select * from profissionais