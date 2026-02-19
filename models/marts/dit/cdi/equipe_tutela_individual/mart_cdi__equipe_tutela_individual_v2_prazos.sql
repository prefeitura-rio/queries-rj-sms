{{ 
    config(
        materialized = 'table',
        schema = 'marts_cdi',
        alias = 'mart_cdi__equipe_tutela_individual_v2_prazos'
    ) 
}}


with base as (

    select *
    from {{ ref('int_cdi__equipe_tutela_individual') }}

),

calculos as (

    select
        *,

        -- vencimento real
        date_add(
            data_de_entrada,
            interval prazo_dias day
        ) as data_vencimento_real,

        -- dias para vencer
        date_diff(
            date_add(data_de_entrada, interval prazo_dias day),
            current_date(),
            day
        ) as dias_para_vencer,

        -- status
        case
            when prazo_dias is null or data_de_entrada is null
                then 'Ignorado'
            when date_add(data_de_entrada, interval prazo_dias day) < current_date()
                then 'Vencido'
            else 'A vencer'
        end as status_vencimento,

        case
            when prazo_dias is null or data_de_entrada is null
                then 'Ignorado'

            when date_add(data_de_entrada, interval prazo_dias day) < current_date()
                then 'Vencido'

            when date_add(data_de_entrada, interval prazo_dias day) = current_date()
                then 'Vence hoje'

            when date_add(data_de_entrada, interval prazo_dias day) > current_date()
                then 'A vencer'
        end as status_prazo_simples,

        case
            when prazo_dias is null or data_de_entrada is null
                then 'Prazo ignorado'

            when date_add(data_de_entrada, interval prazo_dias day) < current_date()
                then concat(
                    'Vencido há ',
                    abs(
                        date_diff(
                            date_add(data_de_entrada, interval prazo_dias day),
                            current_date(),
                            day
                        )
                    ),
                    ' dias'
                )

            else concat(
                'Vence em ',
                date_diff(
                    date_add(data_de_entrada, interval prazo_dias day),
                    current_date(),
                    day
                ),
                ' dias'
            )
        end as descricao_vencimento,
        

    from base
)

select *
from calculos
