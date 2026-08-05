{{
  config(
    enabled=true,
    alias="regulacao_indicadores",
    materialized='table',
  )
}}

/*
  Indicadores de regulação ambulatorial por procedimento, mês, unidade solicitante
  e unidade executante.

  Granularidade: (sigtap_procedimento, mes_referencia, cnes_unidade_solicitante, cnes_unidade_executante)

  - mes_referencia para solicitações: mês de criação da solicitação
  - mes_referencia para execuções/faltas: mês da marcação

  Indicadores:
    1. quantidade_solicitacoes    — nº de solicitações criadas no período
    2. quantidade_execucoes       — nº de marcações executadas no período
    3. quantidade_marcacoes_agendadas — total de marcações com data definida
    4. quantidade_faltas          — nº de faltas registradas
    5. absenteismo_pct            — % faltas / marcações agendadas

  Fonte: raw_sisreg_api_v2__solicitacao_ambulatorial + raw_sisreg_api_v2__marcacao_ambulatorial
*/

with
    -- =========================================================================
    -- BLOCO 1: Solicitações agrupadas por (solicitante, procedimento, mês)
    -- Cobre todos os casos, incluindo os que nunca chegaram a ter marcação.
    -- unidade_executante é null nestas linhas.
    -- =========================================================================
    solicitacoes as (
        select
            unidade_solicitante_id_cnes                             as id_cnes_solicitante,
            unidade_solicitante_nome                                as nome_solicitante_sisreg,
            cast(null as string)                                    as id_cnes_executante,
            cast(null as string)                                    as nome_executante_sisreg,
            procedimento_sigtap_id                                  as sigtap_procedimento,
            coalesce(
                procedimento_sigtap_descricao,
                procedimento_descricao
            )                                                       as nome_procedimento,
            date_trunc(date(solicitacao_datahora), month)           as mes_referencia,
            count(distinct solicitacao_id)                          as quantidade_solicitacoes,
            cast(null as int64)                                     as quantidade_execucoes,
            cast(null as int64)                                     as quantidade_marcacoes_agendadas,
            cast(null as int64)                                     as quantidade_faltas
        from {{ ref('raw_sisreg_api_v2__solicitacao_ambulatorial') }}
        where
            solicitacao_datahora is not null
            and procedimento_sigtap_id is not null
            and unidade_solicitante_id_cnes is not null
            and date(solicitacao_datahora) >= '2025-01-01'
        group by
            id_cnes_solicitante,
            nome_solicitante_sisreg,
            id_cnes_executante,
            nome_executante_sisreg,
            sigtap_procedimento,
            nome_procedimento,
            mes_referencia
    ),

    -- =========================================================================
    -- BLOCO 2: Marcações agrupadas por (solicitante, executante, procedimento, mês)
    -- Já contém ambas as unidades em cada registro.
    -- =========================================================================
    marcacoes as (
        select
            unidade_solicitante_id_cnes                             as id_cnes_solicitante,
            unidade_solicitante_nome                                as nome_solicitante_sisreg,
            unidade_executante_id_cnes                              as id_cnes_executante,
            unidade_executante_nome                                 as nome_executante_sisreg,
            procedimento_sigtap_id                                  as sigtap_procedimento,
            coalesce(
                procedimento_sigtap_descricao,
                procedimento_descricao
            )                                                       as nome_procedimento,
            date_trunc(date(parse_timestamp('%Y-%m-%dT%H:%M:%E6SZ', marcacao_data)), month) as mes_referencia,
            cast(null as int64)                                     as quantidade_solicitacoes,
            countif(marcacao_executada = 'sim')                     as quantidade_execucoes,
            count(*)                                                as quantidade_marcacoes_agendadas,
            countif(flag_falta_registrada = 'sim')                  as quantidade_faltas
        from {{ ref('raw_sisreg_api_v2__marcacao_ambulatorial') }}
        where
            marcacao_data is not null
            and procedimento_sigtap_id is not null
            and unidade_solicitante_id_cnes is not null
            and unidade_executante_id_cnes is not null
            and date(parse_timestamp('%Y-%m-%dT%H:%M:%E6SZ', marcacao_data)) >= '2025-01-01'
        group by
            id_cnes_solicitante,
            nome_solicitante_sisreg,
            id_cnes_executante,
            nome_executante_sisreg,
            sigtap_procedimento,
            nome_procedimento,
            mes_referencia
    ),

    -- =========================================================================
    -- BLOCO 3: FULL OUTER JOIN
    -- Une solicitações e marcações pela chave (solicitante, executante, procedimento, mês).
    -- Solicitações sem marcação terão executante null e métricas de execução zeradas.
    -- =========================================================================
    consolidado as (
        select
            coalesce(s.id_cnes_solicitante, m.id_cnes_solicitante)     as id_cnes_solicitante,
            coalesce(s.nome_solicitante_sisreg, m.nome_solicitante_sisreg) as nome_solicitante_sisreg,
            m.id_cnes_executante,
            m.nome_executante_sisreg,
            coalesce(s.sigtap_procedimento, m.sigtap_procedimento)     as sigtap_procedimento,
            coalesce(s.nome_procedimento, m.nome_procedimento)          as nome_procedimento,
            coalesce(s.mes_referencia, m.mes_referencia)                as mes_referencia,
            coalesce(s.quantidade_solicitacoes, 0)                      as quantidade_solicitacoes,
            coalesce(m.quantidade_execucoes, 0)                         as quantidade_execucoes,
            coalesce(m.quantidade_marcacoes_agendadas, 0)               as quantidade_marcacoes_agendadas,
            coalesce(m.quantidade_faltas, 0)                            as quantidade_faltas
        from solicitacoes as s
        full outer join marcacoes as m
            on  s.id_cnes_solicitante  = m.id_cnes_solicitante
            and s.sigtap_procedimento  = m.sigtap_procedimento
            and s.mes_referencia       = m.mes_referencia
    ),

    -- =========================================================================
    -- BLOCO 4: Enriquecimento com dim_estabelecimento (solicitante e executante)
    -- =========================================================================
    final as (
        select
            -- Dimensão: Procedimento
            consolidado.sigtap_procedimento,
            consolidado.nome_procedimento,

            -- Dimensão: Período
            consolidado.mes_referencia,
            format_date('%m/%Y', consolidado.mes_referencia)           as mes_ano,

            -- Dimensão: Unidade Solicitante
            consolidado.id_cnes_solicitante                            as cnes_unidade_solicitante,
            coalesce(
                dim_sol.nome_acentuado,
                consolidado.nome_solicitante_sisreg
            )                                                           as nome_unidade_solicitante,
            dim_sol.area_programatica                                   as area_programatica_solicitante,
            dim_sol.endereco_latitude                                   as latitude_solicitante,
            dim_sol.endereco_longitude                                  as longitude_solicitante,

            -- Dimensão: Unidade Executante
            consolidado.id_cnes_executante                             as cnes_unidade_executante,
            coalesce(
                dim_exe.nome_acentuado,
                consolidado.nome_executante_sisreg
            )                                                           as nome_unidade_executante,
            dim_exe.area_programatica                                   as area_programatica_executante,
            dim_exe.endereco_latitude                                   as latitude_executante,
            dim_exe.endereco_longitude                                  as longitude_executante,

            -- Indicadores
            consolidado.quantidade_solicitacoes,
            consolidado.quantidade_execucoes,
            consolidado.quantidade_marcacoes_agendadas,
            consolidado.quantidade_faltas,
            case
                when consolidado.quantidade_marcacoes_agendadas > 0
                then round(
                    safe_divide(
                        consolidado.quantidade_faltas,
                        consolidado.quantidade_marcacoes_agendadas
                    ) * 100,
                    2
                )
                else null
            end                                                         as absenteismo_pct

        from consolidado
        left join {{ ref('dim_estabelecimento') }} as dim_sol
            on consolidado.id_cnes_solicitante = dim_sol.id_cnes
        left join {{ ref('dim_estabelecimento') }} as dim_exe
            on consolidado.id_cnes_executante = dim_exe.id_cnes
    )

select * from final
