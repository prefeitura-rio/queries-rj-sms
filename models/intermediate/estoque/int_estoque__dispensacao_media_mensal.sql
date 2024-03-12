select id_cnes, id_material, avg(quantidade_dispensada) as quantidade,
from {{ ref("int_estoque__dispensacao_serie_historica_com_outliers_identificados") }}
where outlier = "nao" and row_num <= 90 -- o CMM é calculado a partir das 90 últimas observações
group by id_cnes, id_material
order by id_cnes, id_material
