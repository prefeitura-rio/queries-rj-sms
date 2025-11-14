{{
    config(
        alias="equipe_tipo",
        schema= "brutos_cnes_gdb"
    )
}}

with
    source as (
        select * from {{ source("brutos_cnes_gdb_staging", "equipe_tipo") }}
    )

select
    cast(TP_EQUIPE as string) as id_equipe_tipo,
    cast(DS_EQUIPE as string) as equipe_descricao,
    cast(CO_GRUPO_EQUIPE as string) as id_equipe_grupo
    
from source