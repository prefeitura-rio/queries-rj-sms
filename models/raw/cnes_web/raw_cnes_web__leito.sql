{{
    config(
        alias="leito_tipo",
    )
}}

with
    source as (
        select * from {{ source("brutos_cnes_web_staging", "tbLeito") }}
    )

select
    safe_cast(CO_LEITO as int64) as id_leito_especialidade,
    safe_cast(DS_LEITO as string) as leito_especialidade,
    safe_cast(TP_LEITO as int64) as id_leito_tipo,
    safe_cast(_data_carga as date format 'DD/MM/YYY') as data_carga,
    safe_cast(_data_snapshot as date format 'DD/MM/YYY') as data_snapshot,
    safe_cast(mes_particao as string) as mes_particao,
    safe_cast(ano_particao as string) as ano_particao,
    concat(ano_particao, '-', mes_particao, '-', '01') as data_particao,
from source