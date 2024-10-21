-- - obtem a data da posição mais recente por unidade de saúde (cnes)
with
    source as (
        select *
        from {{ ref("raw_plataforma_smsrio__estoque_posicao_almoxarifado_aps_dengue") }}
    ),

    estoque_mais_recente as (
        select id_cnes, max(data) as data from source group by id_cnes
    )

select *
from estoque_mais_recente
