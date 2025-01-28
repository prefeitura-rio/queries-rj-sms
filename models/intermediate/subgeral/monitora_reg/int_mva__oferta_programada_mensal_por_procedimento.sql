-- vagas programadas pelos profissionais por procedimento
with
    versao_atual as (
        select *
        from {{ ref("fct_sisreg_oferta_programada_serie_historica") }}
        where
            data_particao = (
                select max(data_particao)
                from {{ ref("fct_sisreg_oferta_programada_serie_historica") }}
            )
    )

select
    -- identificadores
    profissional_executante_cpf as cpf,

    -- pega nome mais atual
    array_agg(
        profissional_executante_nome ignore nulls
        order by procedimento_vigencia_ano desc, procedimento_vigencia_mes desc
        limit 1
    )[offset(0)] as nome,

    id_estabelecimento_executante as id_cnes,

    -- pega nome fantasia mais atual
    array_agg(
        estabelecimento ignore nulls
        order by procedimento_vigencia_ano desc, procedimento_vigencia_mes desc
        limit 1
    )[offset(0)] as estabelecimento_nome,

    id_procedimento_interno as id_procedimento,

    -- pega cbo mais atual
    array_agg(
        id_cbo2002 ignore nulls
        order by procedimento_vigencia_ano desc, procedimento_vigencia_mes desc
        limit 1
    )[offset(0)] as id_cbo_2002,

    -- pega ocupacao mais atual
    array_agg(
        ocupacao ignore nulls
        order by procedimento_vigencia_ano desc, procedimento_vigencia_mes desc
        limit 1
    )[offset(0)] as ocupacao,

    -- pega ocupacao_familia mais atual
    array_agg(
        ocupacao_familia ignore nulls
        order by procedimento_vigencia_ano desc, procedimento_vigencia_mes desc
        limit 1
    )[offset(0)] as ocupacao_familia,

    -- contagem de cbos pelos quais o profissional está oferecendo vagas
    array_length(
        array_agg(
            id_cbo2002 ignore nulls
            order by procedimento_vigencia_ano desc, procedimento_vigencia_mes desc
            limit 1
        )
    ) as id_cbo_2002_qtd_sisreg,

    -- cbos pelos quais o profissional está oferecendo vagas
    string_agg(distinct id_cbo2002, ',') as id_cbo_2002_todos_sisreg,

    procedimento_vigencia_ano as ano,
    procedimento_vigencia_mes as mes,

    -- contagens de vagas
    sum(
        vagas_primeira_vez_qtd + vagas_reserva_qtd
    ) as vagas_programadas_mensal_primeira_vez,
    sum(vagas_retorno_qtd) as vagas_programadas_mensal_retorno,
    sum(vagas_todas_qtd) as vagas_programadas_mensal_todas

from versao_atual
where
    -- filtrando periodo de interesse
    procedimento_vigencia_ano >= 2020

    -- filtrando ocupacoes de interesse
    and regexp_contains(
        id_cbo2002,
        r'^(3222|2251|2235|2231|2252|2232|2236|2234|2237|2515|2253|3251|2238|5152|2239)'
    )
    and id_cbo2002 not in ('225142', '225130', '223293', '223565', '322245')  -- exclui saúde da família
group by cpf, id_cnes, id_procedimento, ano, mes
