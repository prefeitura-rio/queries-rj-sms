select
    id_cnes,
    id_material,
    avg(quantidade_dispensada) as quantidade,
from {{ ref("dispensacao_outliers") }}
where outlier = "nao"
group by id_cnes, id_material
order by id_cnes, id_material
