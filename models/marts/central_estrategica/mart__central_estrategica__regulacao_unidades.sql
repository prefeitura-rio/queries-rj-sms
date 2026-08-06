{{
  config(
    enabled=true,
    alias="regulacao_unidades",
    materialized='table',
  )
}}

/*
  Tabela de unidades de saúde envolvidas no fluxo de regulação,
  enriquecida com coordenadas geográficas e área programática provenientes de dim_estabelecimento.

  Granularidade: um registro por unidade (CNES) distinta encontrada nas solicitações de regulação.

  Fonte: mart_regulacao__solicitacao + dim_estabelecimento
*/

with
    -- Unidades solicitantes
    solicitantes as (
        select distinct
            s.solicitante.unidade_id_cnes   as id_cnes,
            s.solicitante.unidade_nome      as nome_sisreg
        from {{ ref('mart_regulacao__solicitacao') }} as s
        where s.solicitante.unidade_id_cnes is not null
    ),

    -- Unidades executantes (campo dentro de array execucao[])
    executantes as (
        select distinct
            ex.unidade_id_cnes              as id_cnes,
            ex.unidade_nome                 as nome_sisreg
        from {{ ref('mart_regulacao__solicitacao') }} as s,
        unnest(s.execucao) as ex
        where ex.unidade_id_cnes is not null
    ),

    -- União de todas as unidades
    todas_unidades as (
        select id_cnes, nome_sisreg from solicitantes
        union all
        select id_cnes, nome_sisreg from executantes
    ),

    -- Agrega por unidade, mantendo nome mais frequente
    unidades_agrupadas as (
        select
            id_cnes,
            -- Nome mais frequente no SISREG para essa unidade
            array_agg(nome_sisreg order by nome_sisreg)[safe_ordinal(1)] as nome_sisreg
        from todas_unidades
        group by id_cnes
    ),

    final as (
        select
            u.id_cnes,
            coalesce(d.nome_acentuado, u.nome_sisreg)   as nome,
            u.nome_sisreg,
            d.area_programatica,
            d.endereco_latitude                          as latitude,
            d.endereco_longitude                         as longitude
        from unidades_agrupadas as u
        left join {{ ref('dim_estabelecimento') }} as d
            on u.id_cnes = d.id_cnes
    )

select * from final
order by nome
