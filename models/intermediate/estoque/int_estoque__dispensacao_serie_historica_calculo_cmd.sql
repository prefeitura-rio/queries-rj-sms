-- filtra os registros utilizados para o cálculo do CMD
with
    cmd_series as (
        select *
        from
            {{
                ref(
                    "int_estoque__dispensacao_serie_historica_com_outliers_identificados"
                )
            }}
        where data >= date_sub(current_date('America/Sao_Paulo'), interval 60 day)
    )

select *
from cmd_series
