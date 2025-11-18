{{
    config(
        alias="equipe_tipo",
        schema= "brutos_gdb_cnes"
    )
}}

with
    source as (
        select * from {{ source("brutos_gdb_cnes_staging", "NFCES046") }}
    )

select
    cast(TP_EQUIPE as string) as id_equipe_tipo,
    cast(DS_EQUIPE as string) as equipe_descricao,
    cast(CO_GRUPO_EQUIPE as string) as id_equipe_grupo,

    -- Podem ser usados posteriormente para deduplicação
    data_particao,
    _loaded_at as data_carga,

from source
