{{
    config(
        alias = "NFCES058",
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
    'IND_VINC',
    'CD_VINCULACAO',
    'TP_VINCULO',
    'TP_SUBVINCULO',
    'DS_SUBVINCULO',
    'ST_HABILITADO',
    'ST_SOLICITA_CNPJ',
    'TP_CATEGORIA'
] %}

with source as (
    select *
    from {{ source("brutos_gdb_cnes_staging", "NFCES058") }}
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
