{{
  config(
    enabled=true,
    alias="regulacao_indicadores_tempos",
    materialized='table',
  )
}}

/*
  Indicadores de tempo no fluxo de regulação ambulatorial.
  Granularidade: (procedimento_descricao, cnes_unidade_solicitante, cnes_unidade_executante).

  Indicadores calculados:
    - tempo_solicitacao_ate_aprovacao_dias: mediana de dias entre a criação da solicitação
      (solicitacao_datahora) e a aprovação da marcação (marcacao.aprovacao_datahora).
    - tempo_aprovacao_ate_marcacao_dias: mediana de dias entre a aprovação da marcação
      (marcacao.aprovacao_datahora) e a data/hora da marcação (marcacao.datahora),
      que corresponde ao momento em que o procedimento está agendado.
    - quantidade_solicitacoes: número de solicitações no grupo.
    - quantidade_com_aprovacao: número de solicitações que chegaram à etapa de aprovação.
    - quantidade_com_marcacao: número de solicitações com data de marcação definida após aprovação.

  Somente solicitações presentes em mart_historico_clinico_app__regulacao são consideradas
  (já filtrado por critérios de privacidade, ex. HIV).

  Fonte: mart_historico_clinico_app__regulacao + mart_regulacao__solicitacao
         + dim_estabelecimento
*/

with
    -- Base com CNES das unidades e dados de timing
    base as (
        select
            r.solicitacao_id,
            r.procedimento_descricao,
            r.solicitacao_datahora,
            s.solicitante.unidade_id_cnes                       as cnes_solicitante,
            s.solicitante.unidade_nome                          as nome_solicitante_sisreg,
            (
                select ex.unidade_id_cnes
                from unnest(s.execucao) as ex
                where ex.unidade_id_cnes is not null
                limit 1
            )                                                   as cnes_executante,
            (
                select ex.unidade_nome
                from unnest(s.execucao) as ex
                where ex.unidade_id_cnes is not null
                limit 1
            )                                                   as nome_executante_sisreg,
            -- Marcação mais recente com aprovação
            (
                select m.aprovacao_datahora
                from unnest(r.marcacao) as m
                where m.aprovacao_datahora is not null
                order by m.aprovacao_datahora desc
                limit 1
            )                                                   as marcacao_aprovacao_datahora,
            (
                select m.datahora
                from unnest(r.marcacao) as m
                where m.aprovacao_datahora is not null
                order by m.aprovacao_datahora desc
                limit 1
            )                                                   as marcacao_datahora
        from {{ ref('mart_historico_clinico_app__regulacao') }} as r
        inner join {{ ref('mart_regulacao__solicitacao') }} as s
            on r.solicitacao_id = s.solicitacao.id
        where
            r.procedimento_descricao is not null
            and s.solicitante.unidade_id_cnes is not null
    ),

    -- Calcula intervalos em dias por solicitação
    com_tempos as (
        select
            solicitacao_id,
            procedimento_descricao,
            cnes_solicitante,
            nome_solicitante_sisreg,
            cnes_executante,
            nome_executante_sisreg,
            -- Tempo da criação até a aprovação (quando existe aprovação)
            case
                when marcacao_aprovacao_datahora is not null
                    and solicitacao_datahora is not null
                then date_diff(
                    date(marcacao_aprovacao_datahora),
                    date(solicitacao_datahora),
                    day
                )
                else null
            end                                                 as dias_solicitacao_ate_aprovacao,
            -- Tempo da aprovação até a data da marcação (quando ambas existem)
            case
                when marcacao_aprovacao_datahora is not null
                    and marcacao_datahora is not null
                then date_diff(
                    date(marcacao_datahora),
                    date(marcacao_aprovacao_datahora),
                    day
                )
                else null
            end                                                 as dias_aprovacao_ate_marcacao
        from base
    ),

    -- Agrega por (procedimento, solicitante, executante)
    agregado as (
        select
            procedimento_descricao,
            cnes_solicitante,
            nome_solicitante_sisreg,
            cnes_executante,
            nome_executante_sisreg,
            count(distinct solicitacao_id)                      as quantidade_solicitacoes,
            countif(dias_solicitacao_ate_aprovacao is not null) as quantidade_com_aprovacao,
            countif(dias_aprovacao_ate_marcacao is not null)    as quantidade_com_marcacao,
            -- Mediana dos tempos (aproximada via percentil 50)
            approx_quantiles(
                dias_solicitacao_ate_aprovacao, 100
            )[safe_ordinal(51)]                                 as mediana_dias_solicitacao_ate_aprovacao,
            approx_quantiles(
                dias_aprovacao_ate_marcacao, 100
            )[safe_ordinal(51)]                                 as mediana_dias_aprovacao_ate_marcacao,
            -- Média dos tempos como complemento
            round(avg(dias_solicitacao_ate_aprovacao), 1)       as media_dias_solicitacao_ate_aprovacao,
            round(avg(dias_aprovacao_ate_marcacao), 1)          as media_dias_aprovacao_ate_marcacao
        from com_tempos
        group by
            procedimento_descricao,
            cnes_solicitante,
            nome_solicitante_sisreg,
            cnes_executante,
            nome_executante_sisreg
    ),

    -- Enriquece com nomes oficiais via dim_estabelecimento
    final as (
        select
            a.procedimento_descricao,

            a.cnes_solicitante,
            coalesce(
                dim_sol.nome_acentuado,
                a.nome_solicitante_sisreg
            )                                                   as nome_unidade_solicitante,
            dim_sol.area_programatica                           as area_programatica_solicitante,

            a.cnes_executante,
            coalesce(
                dim_exe.nome_acentuado,
                a.nome_executante_sisreg
            )                                                   as nome_unidade_executante,
            dim_exe.area_programatica                           as area_programatica_executante,

            a.quantidade_solicitacoes,
            a.quantidade_com_aprovacao,
            a.quantidade_com_marcacao,
            a.mediana_dias_solicitacao_ate_aprovacao,
            a.mediana_dias_aprovacao_ate_marcacao,
            a.media_dias_solicitacao_ate_aprovacao,
            a.media_dias_aprovacao_ate_marcacao
        from agregado as a
        left join {{ ref('dim_estabelecimento') }} as dim_sol
            on a.cnes_solicitante = dim_sol.id_cnes
        left join {{ ref('dim_estabelecimento') }} as dim_exe
            on a.cnes_executante = dim_exe.id_cnes
    )

select * from final
order by quantidade_solicitacoes desc
