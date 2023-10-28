with
    historico as (select * from {{ ref("int_estoque__dispensacao_serie_historica") }}),
    historico_valido as (
        select
            *,
            row_number() over (
                partition by id_material, id_cnes order by data desc
            ) as row_num,
            percentile_cont(quantidade_dispensada, 0.25) over (
                partition by id_material, id_cnes
            ) as q1,
            percentile_cont(quantidade_dispensada, 0.75) over (
                partition by id_material, id_cnes
            ) as q3,
            (
                percentile_cont(quantidade_dispensada, 0.75) over (
                    partition by id_material, id_cnes
                ) - percentile_cont(quantidade_dispensada, 0.25) over (
                    partition by id_material, id_cnes
                )
            )
            * 1.5 as iqr
        from historico
        where quantidade_dispensada is not null  -- retiramos as datas nas quais não houveram dispensações e não é verificavel se havia estoque positivo
    ),
    historico_ultimos_30 as (select * from historico_valido where row_num <= 30),  -- o CMM é calculado a partir das 30 últimas observações
    outlier as (
        select
            *,
            case
                when
                    quantidade_dispensada < q1 - iqr or quantidade_dispensada > q3 + iqr
                then "sim"
                else "nao"
            end as outlier
        from historico_ultimos_30
    )

select *
from outlier
order by
    id_cnes desc,
    id_material,
    row_num
