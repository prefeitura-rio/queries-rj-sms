{{
    config(
        schema="brutos_sheets",
        alias="assistencial_procedimento",
    )
}}

with source as (
      select * from {{ source('brutos_sheets_staging', 'assistencial_procedimento') }}
),
renamed as (
    select
        {{ adapter.quote("id_procedimento") }},
        {{ adapter.quote("descricao") }},
        safe_cast({{ adapter.quote("parametro_consultas_por_hora") }} as int64) as parametro_consultas_por_hora,

    from source
)
select * from renamed
