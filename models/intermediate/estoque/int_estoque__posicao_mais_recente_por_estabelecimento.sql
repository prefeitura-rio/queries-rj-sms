-- contem a data da poiscao mais recente de cada estabelecimento

with
    posicao as (select * from {{ ref("fct_estoque_posicao") }}),

    posicao_mais_recente_por_estabelecimento as (
        select id_cnes, max(data_particao) as data_particao
        from posicao
        group by id_cnes
    )

select
    id_cnes,
    data_particao,
    concat(id_cnes, "-", data_particao) as id_estabelecimento_particao
from posicao_mais_recente_por_estabelecimento
order by data_particao asc
