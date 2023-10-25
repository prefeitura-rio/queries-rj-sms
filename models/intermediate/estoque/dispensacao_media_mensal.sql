select
    id_cnes,
    id_material,
    avg(quantidade_dispensada) as quantidade,
from {{ ref("dispensacao_serie_historica_com_outliers_identificados") }}
where outlier = "nao"
group by id_cnes, id_material
order by id_cnes, id_material
