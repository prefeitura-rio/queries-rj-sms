{{
    config(
        alias="tipo_vinculo_empregaticio",
    )
}}

with source as (select * from {{ source("brutos_cnes_web_staging", "tbTpModVinculo") }})
select
    safe_cast(cd_vinculacao as string) as id_vinculacao,
    safe_cast(tp_vinculo as string) as tipo,
    safe_cast(ds_vinculo as string) as descricao,
    safe_cast(ano_particao as string) as ano_particao,
    concat(
        safe_cast(ano_particao as string), '-', safe_cast(mes_particao as string), '-01'
    ) as data_particao,
    safe_cast(_data_carga as datetime) as data_carga,
    safe_cast(_data_snapshot as datetime) as data_snapshot
from source
