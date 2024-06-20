{{
    config(
        alias="segmento",
    )
}}

with
    source as (
        select * from {{ source("brutos_cnes_web_staging", "tbSegmento") }}
    )

select
    safe_cast(co_municipio as string) as id_municipio,
    safe_cast(co_segmento as string) as id_segmento,
    safe_cast(ds_segmento as string) as segmento_descricao,
    safe_cast(tp_segmento as string) as segmento_tipo,
    safe_cast(dt_atualizacao as date format 'DD/MM/YYY') as data_atualizacao,
    safe_cast(dt_atualizacao_origem as date format 'DD/MM/YYY') as data_atualizacao_origem,
    safe_cast(co_usuario as string) as usuario,
    safe_cast(_data_carga as date format 'DD/MM/YYY') as data_carga,
    safe_cast(_data_snapshot as date format 'DD/MM/YYY') as data_snapshot,
    safe_cast(mes_particao as string) as mes_particao,
    safe_cast(ano_particao as string) as ano_particao,
    concat(ano_particao, '-', mes_particao, '-', '01') as data_particao,
from source