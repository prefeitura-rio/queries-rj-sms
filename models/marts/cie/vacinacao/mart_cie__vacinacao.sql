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
    vacinacoes_agg as (
        select *, 'api' as origem  from {{ ref('intermediate_cie__vacinacao_api') }}
        union all
        select *, 'historico' as origem from {{ ref('intermediate_cie__vacinacao_historico') }}
    ),

    vacinacoes_dedup as (
        select 
            *
        from vacinacoes_agg
        qualify row_number() over (
            partition by id_vacinacao
            order by 
                case when origem = 'api' then 1 else 2 end 
            desc
        ) = 1
    )

select *
from vacinacoes_dedup
   