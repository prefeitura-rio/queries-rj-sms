{{
    config(
        alias="vacinacao",
        materialized="table",
        partition_by={
            "field": "particao_data_vacinacao",
            "data_type": "date",
            "granularity": "month"
        }
    )
}}

with
    continuo_e_historico as (
        select *, 'historico' as origem from {{ ref('int_cie__vacinacao_historico') }}
        union all
        select *, 'continuo' as origem from {{ ref('int_cie__vacinacao_continuo') }}
    ),

    api as (
        select *, 'api' as origem
        from {{ ref('int_cie__vacinacao_api') }}
        where id_vacinacao not in (
            select id_vacinacao
            from continuo_e_historico
        )
    ),

    vacinacoes as (
        select * from continuo_e_historico
        union all
        select * from api
    ),


    vacinacoes_dedup as (
        select 
            *
        from vacinacoes
        qualify row_number() over (
            partition by id_vacinacao
            order by 
                case 
                    when origem = 'api' then 1 
                    when origem = 'historico' then 2 
                    else 3 
                end
        ) = 1
    )

select *
from vacinacoes_dedup
   