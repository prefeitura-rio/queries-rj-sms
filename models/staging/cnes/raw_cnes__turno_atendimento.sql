{{
    config(
        alias="turno_atendimento",
    )
}}

with source as (select * from {{ source("brutos_cnes_staging", "tbTurnoAtendimento") }})
select
    -- Primary key
    safe_cast(co_turno_atendimento as string) as id_turno_atendimento,

    -- Common fields
    safe_cast(ds_turno_atendimento as string) as descricao,

    -- Metadata
    safe_cast(_data_carga as datetime) as data_carga,
    safe_cast(_data_snapshot as datetime) as data_snapshot
from source
