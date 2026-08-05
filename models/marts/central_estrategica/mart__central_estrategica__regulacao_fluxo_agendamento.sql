{{
  config(
    enabled=true,
    alias="regulacao_fluxo_agendamento",
    materialized='table',
  )
}}

/*
  Fluxo de agendamentos entre unidades solicitantes e executantes.
  Granularidade: (unidade_solicitante, unidade_executante, procedimento, mês).

  Foco: quantidade de marcações agendadas por par de unidades.
  Mês de referência: data da marcação (quando o atendimento estava agendado).

  Fonte: raw_sisreg_api_v2__marcacao_ambulatorial
*/

with
    marcacoes as (
        select
            -- Procedimento
            procedimento_sigtap_id                                          as sigtap_procedimento,
            coalesce(
                procedimento_sigtap_descricao,
                procedimento_descricao
            )                                                               as nome_procedimento,
            procedimento_grupo_codigo                                       as sigtap_grupo_codigo,
            procedimento_grupo_nome                                         as nome_grupo,

            -- Período
            date_trunc(date(parse_timestamp('%Y-%m-%dT%H:%M:%E6SZ', marcacao_data)), month) as mes_referencia,

            -- Unidade Solicitante
            unidade_solicitante_id_cnes                                     as cnes_unidade_solicitante,
            unidade_solicitante_nome                                        as nome_unidade_solicitante_sisreg,

            -- Unidade Executante
            unidade_executante_id_cnes                                      as cnes_unidade_executante,
            unidade_executante_nome                                         as nome_unidade_executante_sisreg,

            -- Métricas
            count(*)                                                        as quantidade_agendada
        from {{ ref('raw_sisreg_api_v2__marcacao_ambulatorial') }}
        where
            marcacao_data is not null
            and procedimento_sigtap_id is not null
            and unidade_solicitante_id_cnes is not null
            and unidade_executante_id_cnes is not null
            and date(parse_timestamp('%Y-%m-%dT%H:%M:%E6SZ', marcacao_data)) >= '2025-01-01'
        group by
            sigtap_procedimento,
            nome_procedimento,
            sigtap_grupo_codigo,
            nome_grupo,
            mes_referencia,
            cnes_unidade_solicitante,
            nome_unidade_solicitante_sisreg,
            cnes_unidade_executante,
            nome_unidade_executante_sisreg
    ),

    final as (
        select
            -- Procedimento
            m.sigtap_procedimento,
            m.nome_procedimento,
            m.sigtap_grupo_codigo,
            m.nome_grupo,

            -- Período
            m.mes_referencia,
            format_date('%m/%Y', m.mes_referencia)                          as mes_ano,

            -- Unidade Solicitante
            m.cnes_unidade_solicitante,
            coalesce(dim_sol.nome_acentuado, m.nome_unidade_solicitante_sisreg) as nome_unidade_solicitante,
            dim_sol.area_programatica                                       as area_programatica_solicitante,
            dim_sol.endereco_latitude                                       as latitude_solicitante,
            dim_sol.endereco_longitude                                      as longitude_solicitante,

            -- Unidade Executante
            m.cnes_unidade_executante,
            coalesce(dim_exe.nome_acentuado, m.nome_unidade_executante_sisreg) as nome_unidade_executante,
            dim_exe.area_programatica                                       as area_programatica_executante,
            dim_exe.endereco_latitude                                       as latitude_executante,
            dim_exe.endereco_longitude                                      as longitude_executante,

            -- Métrica
            m.quantidade_agendada

        from marcacoes as m
        left join {{ ref('dim_estabelecimento') }} as dim_sol
            on m.cnes_unidade_solicitante = dim_sol.id_cnes
        left join {{ ref('dim_estabelecimento') }} as dim_exe
            on m.cnes_unidade_executante = dim_exe.id_cnes
    )

select * from final
