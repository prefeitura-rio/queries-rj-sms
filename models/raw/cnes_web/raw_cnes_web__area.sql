{{
    config(
        alias="area",
    )
}}

with
    source as (
        select * from {{ source("brutos_cnes_web_staging", "tbArea") }}
    )

select
    safe_cast(co_municipio as string) as id_municipio,
    safe_cast(co_area as string) as id_area,
    safe_cast(ds_area as string) as area_descricao,
    safe_cast(cd_segmento as string) as id_segmento,
    safe_cast(dt_atualizacao as date format 'DD/MM/YYY') as data_atualizacao,
    safe_cast(dt_atualizacao_origem as date format 'DD/MM/YYY') as data_atualizacao_origem,
    safe_cast(co_usuario as string) as usuario,
    safe_cast(_data_carga as date format 'DD/MM/YYY') as data_carga,
    safe_cast(_data_snapshot as date format 'DD/MM/YYY') as data_snapshot,
    safe_cast(mes_particao as string) as mes_particao,
    safe_cast(ano_particao as string) as ano_particao,
    concat(ano_particao, '-', mes_particao, '-', '01') as data_particao,
from source