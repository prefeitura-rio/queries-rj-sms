{{
    config(
        schema = 'intermediario_plataforma_subpav',
        alias = 'cnes_aps__competencias_legado',
        materialized = "table",
        tags = ["subpav", "cnes_aps"]
    )
}}

with source as (
    select *
    from {{ source("brutos_plataforma_subpav_staging", "subpav_cnes__competencias") }}
),

sem_duplicatas as (
    select *
    from source
    qualify row_number() over (
        partition by safe_cast(id as int64)
        order by safe_cast(updated_at as timestamp) desc
    ) = 1
),

parse as (
    select
        safe_cast(id as int64) as competencia_id,
        cast(ds_competencia as string) as competencia,
        date(concat(cast(ds_competencia as string), '-01')) as data_particao,

        coalesce(
            safe_cast(dt_final as date),
            safe.parse_date('%Y-%m-%d', cast(dt_final as string)),
            safe.parse_date('%d/%m/%Y', cast(dt_final as string))
        ) as dt_final_competencia,

        safe_cast(base_final as int64) as base_final,
        safe_cast(created_at as timestamp) as created_at,
        safe_cast(updated_at as timestamp) as updated_at

    from sem_duplicatas
    where ds_competencia is not null
),

final as (
    select
        *,

        lag(dt_final_competencia) over (
            order by data_particao
        ) as dt_final_competencia_anterior

    from parse
)

select *
from final