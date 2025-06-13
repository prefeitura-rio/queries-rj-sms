{{
    config(
        schema="brutos_sheets",
        alias="assistencial_procedimento",
    )
}}

with
    source as (
        select
            lpad(id_procedimento, 7, "0") as id_procedimento,
            descricao,
            safe_cast(
                parametro_consultas_por_hora as float64
            ) as parametro_consultas_por_hora,
            safe_cast(parametro_reservas as float64) as parametro_reservas,
            safe_cast(parametro_retornos as float64) as parametro_retornos,
            upper(especialidade) as especialidade,
            upper(tipo_procedimento) as tipo_procedimento

        from {{ source("brutos_sheets_staging", "assistencial_procedimento") }}
    )

select *
from source
