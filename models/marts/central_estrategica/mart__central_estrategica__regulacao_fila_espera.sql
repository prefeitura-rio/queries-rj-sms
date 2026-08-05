{{
  config(
    enabled=true,
    alias="regulacao_fila_espera",
    materialized='table',
  )
}}

/*
  Fila de espera por agendamento no SISREG.
  Granularidade: (unidade_solicitante, procedimento).

  Conta solicitações com status 'SOLICITAÇÃO / AUTORIZADA / REGULADOR':
  foram autorizadas pelo regulador mas ainda não têm data de marcação definida.

  Fonte: raw_sisreg_api_v2__solicitacao_ambulatorial
*/

with
    fila as (
        select
            -- Procedimento
            procedimento_sigtap_id                                          as sigtap_procedimento,
            coalesce(
                procedimento_sigtap_descricao,
                procedimento_descricao
            )                                                               as nome_procedimento,
            procedimento_grupo_codigo                                       as sigtap_grupo_codigo,
            procedimento_grupo_nome                                         as nome_grupo,

            -- Unidade Solicitante
            unidade_solicitante_id_cnes                                     as cnes_unidade_solicitante,
            unidade_solicitante_nome                                        as nome_unidade_solicitante_sisreg,

            -- Métrica
            count(distinct solicitacao_id)                                  as quantidade_aguardando_agendamento

        from {{ ref('raw_sisreg_api_v2__solicitacao_ambulatorial') }}
        where
            solicitacao_status = 'SOLICITAÇÃO / AUTORIZADA / REGULADOR'
            and procedimento_sigtap_id is not null
            and unidade_solicitante_id_cnes is not null
        group by
            sigtap_procedimento,
            nome_procedimento,
            sigtap_grupo_codigo,
            nome_grupo,
            cnes_unidade_solicitante,
            nome_unidade_solicitante_sisreg
    ),

    final as (
        select
            -- Procedimento
            f.sigtap_procedimento,
            f.nome_procedimento,
            f.sigtap_grupo_codigo,
            f.nome_grupo,

            -- Unidade Solicitante
            f.cnes_unidade_solicitante,
            coalesce(
                dim.nome_acentuado,
                f.nome_unidade_solicitante_sisreg
            )                                                               as nome_unidade_solicitante,
            dim.area_programatica                                           as area_programatica_solicitante,
            dim.endereco_latitude                                           as latitude_solicitante,
            dim.endereco_longitude                                          as longitude_solicitante,

            -- Métrica
            f.quantidade_aguardando_agendamento

        from fila as f
        left join {{ ref('dim_estabelecimento') }} as dim
            on f.cnes_unidade_solicitante = dim.id_cnes
    )

select * from final
