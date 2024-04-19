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
        descricao,
        id_procedimento,
        safe_cast(parametro_consultas_por_hora as float64) as parametro_consultas_por_hora,
        safe_cast(parametro_reservas as float64) as parametro_reservas,
        safe_cast(parametro_retornos as float64) as parametro_retornos

    from source
)
select * from renamed