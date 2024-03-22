with
    cmd_sem_outliers as (
        select id_cnes, id_material, avg(quantidade_dispensada) as quantidade,
        from
            {{
                ref(
                    "int_estoque__dispensacao_serie_historica_com_outliers_identificados"
                )
            }}
        where outlier = "nao" and row_num <= 90  -- o CMD é calculado a partir das 90 últimas observações
        group by id_cnes, id_material
        order by id_cnes, id_material
    ),

    cmd_com_outliers as (
        select id_cnes, id_material, avg(quantidade_dispensada) as quantidade,
        from
            {{
                ref(
                    "int_estoque__dispensacao_serie_historica_com_outliers_identificados"
                )
            }}
        where row_num <= 90  -- o CMD é calculado a partir das 90 últimas observações
        group by id_cnes, id_material
        order by id_cnes, id_material
    )

select
    cmd_sem_outliers.id_cnes,
    cmd_sem_outliers.id_material,
    if(
        cmd_sem_outliers.quantidade is null or cmd_sem_outliers.quantidade = 0,
        cmd_com_outliers.quantidade,
        cmd_sem_outliers.quantidade
    ) as quantidade,  -- para eventos esporádicos, onde todos os pontos são considerados outliers, o CMD é calculado com todos os pontos
    -- cmd_com_outliers.quantidade as qtd_com,
    -- cmd_sem_outliers.quantidade as qtd_sem
from cmd_sem_outliers
left join
    cmd_com_outliers
    on cmd_sem_outliers.id_cnes = cmd_com_outliers.id_cnes
    and cmd_sem_outliers.id_material = cmd_com_outliers.id_material
