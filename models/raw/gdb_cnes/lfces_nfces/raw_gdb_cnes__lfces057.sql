{{
    config(
        alias = "LFCES057",
        schema = "brutos_gdb_cnes",
        partition_by = {
            "field": "data_particao",
            "data_type": "date",
            "granularity": "month",
        },
        tags = ["raw", "gdb_cnes", "lfces_nfces"]
    )
}}

{% set fields = [
    'CNES',
    'COD_MUN',
    'COMPETENCIA',
    'CD_MOTIVO_DESAB',
    'TP_GESTAO',
    'STATUS_ESTAB',
    'ST_ESTRUTURA'
] %}

with source as (
    select *
    from {{ source("brutos_gdb_cnes_staging", "LFCES057") }}
),

extracted as (
    select
        {{ json_fields(fields) }},

        cast(_source_file as string) as _source_file,
        safe_cast(_loaded_at as timestamp) as _loaded_at,
        safe_cast(data_particao as date) as data_particao,
        safe_cast(ano_particao as int64) as ano_particao,
        safe_cast(mes_particao as int64) as mes_particao
    from source
)

select *
from extracted
