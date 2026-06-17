{{ 
    config(
        materialized = 'table',
        schema = 'projeto_cdi',
        alias = 'equipe_tutela_individual_prazos',
        meta={"owner": "karen"}
    ) 
}}

with base as (

    select *
    from {{ ref('int_cdi__tutela_individual') }}

),

calculos as (

    select
        *,

        date_add(
            data_entrada,
            interval prazo_dias day
        ) as data_vencimento_real,

        date_diff(
            date_add(data_entrada, interval prazo_dias day),
            current_date(),
            day
        ) as dias_para_vencer,

        case
            when situacao <> "RESOLVIDO" then null
            when data_finalizacao <= date_add(data_entrada, interval prazo_dias day)
                then 'Dentro do Prazo'
            else 'Fora do Prazo'
        end as situacao_prazo,

        case
            when situacao = "RESOLVIDO" then "Finalizado"

            when date_add(data_entrada, interval prazo_dias day) < current_date()
                then concat(
                    'Vencido há ',
                    abs(
                        date_diff(
                            date_add(data_entrada, interval prazo_dias day),
                            current_date(),
                            day
                        )
                    ),
                    ' dias'
                )

            else concat(
                'Vence em ',
                date_diff(
                    date_add(data_entrada, interval prazo_dias day),
                    current_date(),
                    day
                ),
                ' dias'
            )
        end as descricao_vencimento

    from base
)

select *
from calculos