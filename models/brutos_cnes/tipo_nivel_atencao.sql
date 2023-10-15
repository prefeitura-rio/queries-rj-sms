with source as (select * from {{ source("brutos_cnes_staging", "tbGestao") }})

select
    -- Primary key
    safe_cast(co_gestao as string) as id_nivel_atencao,

    -- Common fields
    safe_cast(ds_gestao as string) as descricao,
    safe_cast(tp_prog as string) as tipo,

    -- Metadata
    safe_cast(_data_carga as string) as data_carga,
    safe_cast(_data_snapshot as string) as data_snapshot
from source
