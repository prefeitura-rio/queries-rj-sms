with
    cmd_sem_outliers as (
        select
            id_cnes,
            id_material,
            avg(quantidade_dispensada) as quantidade,
            sum(
                case when quantidade_dispensada is not null then 1 end
            ) as qtd_observacoes,
        from {{ ref("int_estoque__dispensacao_serie_historica_calculo_cmd") }}
        where outlier = "nao"
        group by id_cnes, id_material
        order by id_cnes, id_material
    ),

    cmd_com_outliers as (
        select
            id_cnes,
            id_material,
            avg(quantidade_dispensada) as quantidade,
            sum(
                case when quantidade_dispensada is not null then 1 end
            ) as qtd_observacoes,
        from {{ ref("int_estoque__dispensacao_serie_historica_calculo_cmd") }}
        group by id_cnes, id_material
        order by id_cnes, id_material
    ),

    remove_cmd_invalidos as (
        select
            cmd_sem_outliers.id_cnes,
            cmd_sem_outliers.id_material,

            -- removidos os casos de cmd negativos e com menos de 20 observações
            if(
                cmd_com_outliers.quantidade < 0
                or cmd_com_outliers.qtd_observacoes < 20,
                null,
                round(cmd_com_outliers.quantidade, 2)
            ) as cmd_com_outliers,

            cmd_com_outliers.qtd_observacoes as qtd_observacoes_com_outliers,

            if(
                cmd_sem_outliers.quantidade < 0
                or cmd_sem_outliers.qtd_observacoes < 20,
                null,
                round(cmd_sem_outliers.quantidade, 2)
            ) as cmd_sem_outliers,

            cmd_sem_outliers.qtd_observacoes as qtd_observacoes_sem_outliers,
        from cmd_sem_outliers
        left join
            cmd_com_outliers
            on cmd_sem_outliers.id_cnes = cmd_com_outliers.id_cnes
            and cmd_sem_outliers.id_material = cmd_com_outliers.id_material
    ),

    adiciona_cmd_hibrido as (
        select
            *,
            if(
                cmd_sem_outliers is null or cmd_sem_outliers = 0,
                cmd_com_outliers,
                cmd_sem_outliers
            ) as cmd_hibrido,  -- para eventos esporádicos, onde todos os pontos são considerados outliers, o CMD é calculado com todos os pontos
        from remove_cmd_invalidos
    )

select *
from adiciona_cmd_hibrido
