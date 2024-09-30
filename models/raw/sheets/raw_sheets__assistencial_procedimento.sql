{{
    config(
        schema="brutos_sheets",
        alias="assistencial_procedimento",
    )
}}

with
source as (
    select 
        id_procedimento,
        descricao,
        parametro_consultas_por_hora,
        parametro_reservas,
        parametro_retornos

    from {{ source("brutos_sheets_staging", "assistencial_procedimento") }}
)

select * from source