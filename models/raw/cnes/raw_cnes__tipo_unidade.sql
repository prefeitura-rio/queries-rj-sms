{{
    config(
        alias="tipo_unidade",
    )
}}

with source as (select * from {{ source("brutos_cnes_staging", "tbTipoUnidade") }})

select
    -- Primary Key
    safe_cast(co_tipo_unidade as string) as id_tipo_unidade,

    -- Foreign Keys
    -- Common fields
    safe_cast(ds_tipo_unidade as string) as descricao,

    -- Metadata
    safe_cast(_data_snapshot as date) as data_snapshot,
    safe_cast(_data_carga as datetime) as data_carga,
from source
order by id_tipo_unidade
