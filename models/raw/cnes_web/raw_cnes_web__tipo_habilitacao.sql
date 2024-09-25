{{
    config(
        alias="tipo_habilitacao",
    )
}}

with
source as (
    select * from {{ source("brutos_cnes_web_staging", "tbSubGruposHabilitacao") }}
)

select
    safe_cast(CO_CODIGO_GRUPO as int64) as id_habilitacao,
    UPPER(NO_DESCRICAO_GRUPO) as habilitacao,
    TP_ORIGEM as tipo_origem,
    TP_HABILITACAO as tipo_habilitacao,
    safe_cast(_data_carga as date format 'DD/MM/YYY') as data_carga,
    safe_cast(_data_snapshot as date format 'DD/MM/YYY') as data_snapshot,
    safe_cast(mes_particao as string) as mes_particao,
    safe_cast(ano_particao as string) as ano_particao,
    concat(ano_particao, '-', mes_particao, '-', '01') as data_particao,
from source