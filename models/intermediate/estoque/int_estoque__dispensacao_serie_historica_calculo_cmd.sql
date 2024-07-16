-- filtra os registros utilizados para o cálcuo do CMD
with
    cmd_series as (
        select *
        from
            {{
                ref(
                    "int_estoque__dispensacao_serie_historica_com_outliers_identificados"
                )
            }}
        where row_num <= 60
    )

select *
from cmd_series
