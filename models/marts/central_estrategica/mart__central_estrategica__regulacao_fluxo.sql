{{
  config(
    enabled=true,
    alias="regulacao_fluxo",
    materialized='table',
  )
}}

/*
  Fluxo de regulação entre unidades solicitantes e executantes.
  Granularidade: (cnes_unidade_solicitante, cnes_unidade_executante).

  Contabiliza a quantidade de solicitações que percorreram cada par
  solicitante → executante. Inclui coordenadas geográficas para
  visualização cartográfica do fluxo.

  Fonte: mart_historico_clinico_app__regulacao + mart_regulacao__solicitacao
         + dim_estabelecimento
*/

with
    -- Extrai par (solicitante, executante) de cada solicitação
    pares as (
        select
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
            )                                                   as nome_executante_sisreg
        from {{ ref('mart_historico_clinico_app__regulacao') }} as r
        inner join {{ ref('mart_regulacao__solicitacao') }} as s
            on r.solicitacao_id = s.solicitacao.id
        where s.solicitante.unidade_id_cnes is not null
    ),

    -- Agrega quantidade por par
    fluxo as (
        select
            cnes_solicitante,
            nome_solicitante_sisreg,
            cnes_executante,
            nome_executante_sisreg,
            count(*)                                            as quantidade_solicitacoes
        from pares
        where cnes_executante is not null
        group by
            cnes_solicitante,
            nome_solicitante_sisreg,
            cnes_executante,
            nome_executante_sisreg
    ),

    -- Enriquece com dim_estabelecimento
    final as (
        select
            f.cnes_solicitante,
            coalesce(
                dim_sol.nome_acentuado,
                f.nome_solicitante_sisreg
            )                                                   as nome_unidade_solicitante,
            dim_sol.area_programatica                           as area_programatica_solicitante,
            dim_sol.endereco_latitude                           as latitude_solicitante,
            dim_sol.endereco_longitude                          as longitude_solicitante,

            f.cnes_executante,
            coalesce(
                dim_exe.nome_acentuado,
                f.nome_executante_sisreg
            )                                                   as nome_unidade_executante,
            dim_exe.area_programatica                           as area_programatica_executante,
            dim_exe.endereco_latitude                           as latitude_executante,
            dim_exe.endereco_longitude                          as longitude_executante,

            f.quantidade_solicitacoes
        from fluxo as f
        left join {{ ref('dim_estabelecimento') }} as dim_sol
            on f.cnes_solicitante = dim_sol.id_cnes
        left join {{ ref('dim_estabelecimento') }} as dim_exe
            on f.cnes_executante = dim_exe.id_cnes
    )

select * from final
order by quantidade_solicitacoes desc
