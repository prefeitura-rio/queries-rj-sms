with

padronizacao_procedimentos as (
    select 
        id_procedimento,
        descricao as procedimento,
        parametro_consultas_por_hora as procedimento_consultas_hora,
        parametro_reservas as procedimento_proporcao_reservas,
        parametro_retornos as procedimento_proporcao_retornos

    from {{ ref("raw_sheets__assistencial_procedimento") }}
),

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
        data_particao = (select max(data_particao) as versao from {{ ref("fct_sisreg_oferta_programada_serie_historica") }})

        -- selecionando periodo de interesse
        and procedimento_vigencia_ano >= 2020

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
        profissionais.cpf,
        profissionais.cns,
        profissionais.nome as profissional,
        profissionais.id_cbo as id_cbo_2002,
        upper(profissionais.cbo) as ocupacao,
        upper(profissionais.cbo_familia) as ocupacao_agg,
        round(sum((profissionais.carga_horaria_ambulatorial * 4.5))) as carga_horaria_ambulatorial_mensal,
        profissionais.ano,
        profissionais.mes,
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

    from {{ref("dim_profissional_sus_rio_historico")}}
    where
        -- selecionando os registros mais atuais
        metadados.data_particao = (select max(metadados.data_particao) from {{ ref("dim_profissional_sus_rio_historico") }})

        -- filtrando por anos de interesse
        and profissionais.ano >= 2020

        -- filtrando por estabelecimentos que possuem oferta no sisreg
        and estabelecimentos.id_cnes in (select distinct id_cnes from oferta_programada_mensal)

        -- selecionando profissionais de interesse
        and estabelecimentos.estabelecimento_sms_indicador = 1
        and profissionais.carga_horaria_ambulatorial > 0
        and regexp_contains(profissionais.id_cbo, r'^(3222|2251|2235|2231|2252|2232|2236|2234|2237|2515|2253|3251|2238|5152|2239)')
        and profissionais.id_cbo not in ("225142", "225130", "223293", "223565", "322245")

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
),


mva as (
    select 
        coalesce(ofer.cpf, prof.cpf) as cpf,
        prof.profissional,
        coalesce(ofer.id_cbo_2002, prof.id_cbo_2002) as id_cbo_2002,
        prof.ocupacao,
        prof.ocupacao_agg,
        coalesce(ofer.id_cnes, prof.id_cnes) as id_cnes,
        prof.cns,
        prof.estabelecimento,
        prof.esfera as esfera_estabelecimento,
        prof.natureza_juridica as natureza_juridica_estabelecimento,
        prof.tipo_gestao as tipo_gestao_estabelecimento,
        prof.turno as turno_estabelecimento,
        prof.tipo as tipo_estabelecimento,
        prof.tipo_unidade_alternativo as tipo_estabelecimento_alternativo,
        prof.tipo_unidade_agrupado as tipo_estabelecimento_agrupado,
        prof.id_ap as id_ap_estabelecimento,
        prof.ap as ap_estabelecimento,
        prof.endereco_bairro as endereco_bairro_estabelecimento,
        ofer.id_procedimento,
        coalesce(ofer.ano, prof.ano) as ano,
        coalesce(ofer.mes, prof.mes) as mes,

        prof.carga_horaria_ambulatorial_mensal,
        case 
            when ofer.vagas_programadas_mensal_todas_unidade is not NULL and prof.carga_horaria_ambulatorial_mensal is NULL then NULL
            when ofer.vagas_programadas_mensal_todas_unidade is NULL and prof.carga_horaria_ambulatorial_mensal is not NULL then prof.carga_horaria_ambulatorial_mensal
            else round(prof.carga_horaria_ambulatorial_mensal * ofer.procedimento_distribuicao)
        end as carga_horaria_procedimento_esperada_mensal,

        -- primeira vez
        ofer.vagas_programadas_mensal_primeira_vez,
        ofer.vagas_programadas_mensal_primeira_vez_unidade,

        -- retorno
        ofer.vagas_programadas_mensal_retorno,
        ofer.vagas_programadas_mensal_retorno_unidade,

        -- total
        ofer.vagas_programadas_mensal_todas,
        ofer.vagas_programadas_mensal_todas_unidade,

        ofer.procedimento_distribuicao,

        case 
            when ofer.vagas_programadas_mensal_todas_unidade is NULL then 0
            else  1
        end as sisreg_dados,

        case 
            when prof.carga_horaria_ambulatorial_mensal is NULL then 0
            else  1
        end as cnes_dados

    from oferta_programada_mensal as ofer
    full outer join profissionais as prof
    using (cpf, id_cnes, ano, mes, id_cbo_2002)
)

select * from mva