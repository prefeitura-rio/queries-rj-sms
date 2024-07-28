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
        where
            data >= date_sub(current_date('America/Sao_Paulo'), interval 60 day)
            and {{ dbt_date.day_of_week("data") }} <> 7  -- antenção primária não abre as domingos
    )

select *
from cmd_series
