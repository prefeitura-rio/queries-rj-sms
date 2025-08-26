-- contem a data da poiscao mais recente de cada estabelecimento
with

    estabelecimentos as (select * from {{ ref("dim_estabelecimento") }}),

    vitacare_posicao_mais_recente_por_estabelecimento as (
        select id_cnes, max(data_particao) as data_particao
        from {{ ref("raw_prontuario_vitacare_api__estoque_posicao") }}
        where material_quantidade > 0
        group by id_cnes
    ),

    vitai_posicao_mais_recente_por_estabelecimento as (
        select id_cnes, max(data_particao) as data_particao
        from {{ ref("raw_prontuario_vitai__estoque_posicao") }}
        where material_quantidade > 0
        group by id_cnes
    ),

    tpc_posicao_mais_recente_por_estabelecimento as (
        select 'tpc' as id_cnes, max(data_particao) as data_particao
        from {{ ref("raw_estoque_central_tpc__estoque_posicao") }}
        where material_quantidade > 0
        group by id_cnes
    ),

    posicao_mais_recente_por_estabelecimento as (
        select *
        from vitacare_posicao_mais_recente_por_estabelecimento
        union all
        select *
        from vitai_posicao_mais_recente_por_estabelecimento
        union all
        select *
        from tpc_posicao_mais_recente_por_estabelecimento
    ),

    final as (
        select
            pos.id_cnes,
            est.nome_limpo,
            est.prontuario_versao,
            pos.data_particao,
            concat(pos.id_cnes, "-", pos.data_particao) as id_estabelecimento_particao
        from posicao_mais_recente_por_estabelecimento as pos
        left join estabelecimentos as est on est.id_cnes = pos.id_cnes
        order by pos.data_particao asc
    )

select *
from final
