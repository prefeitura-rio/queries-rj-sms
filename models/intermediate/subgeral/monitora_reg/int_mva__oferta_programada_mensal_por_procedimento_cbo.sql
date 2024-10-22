-- vagas programadas pelos profissionais por procedimento e ocupação
-- view

with versao_atual as (
    select *
    from {{ ref('fct_sisreg_oferta_programada_serie_historica') }}
    where data_particao = (
        select max(data_particao)
        from {{ ref('fct_sisreg_oferta_programada_serie_historica') }}
    )
)

select
    -- identificadores
    profissional_executante_cpf as cpf,
    id_estabelecimento_executante as id_cnes,
    id_procedimento_interno as id_procedimento,
    id_cbo2002 as id_cbo_2002,
    procedimento_vigencia_ano as ano,
    procedimento_vigencia_mes as mes,

    -- contagens de vagas
    sum((vagas_primeira_vez_qtd + vagas_reserva_qtd)) as vagas_programadas_mensal_primeira_vez,
    sum(vagas_retorno_qtd) as vagas_programadas_mensal_retorno,
    sum(vagas_todas_qtd) as vagas_programadas_mensal_todas

from versao_atual

where
    -- filtrando periodo de interesse
    procedimento_vigencia_ano >= 2020

    -- filtrando ocupacoes de interesse
    and regexp_contains(id_cbo2002, r'^(3222|2251|2235|2231|2252|2232|2236|2234|2237|2515|2253|3251|2238|5152|2239)')
    and id_cbo2002 not in ('225142', '225130', '223293', '223565', '322245') -- exclui saude da familia

group by
    cpf,
    id_cnes,
    id_procedimento,
    id_cbo_2002,
    ano,
    mes