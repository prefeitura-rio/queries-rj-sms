with
    ultima_dispensacao as (
        select id_cnes, id_material, max(data_particao) as data_ultima_dispensacao
        from {{ ref("fct_estoque_movimento") }}
        where movimento_tipo_grupo = "CONSUMO"
        group by id_cnes, id_material
    )

select
    concat(id_cnes, "-", id_material) as id_cnes_material,
    *,
    date_diff(current_date('America/Sao_Paulo'), data_ultima_dispensacao, day) as dias_ultima_dispensacao
from ultima_dispensacao
where date_diff(current_date('America/Sao_Paulo'), data_ultima_dispensacao, day) > 90
