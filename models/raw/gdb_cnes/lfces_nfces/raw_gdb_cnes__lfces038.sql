{{
    config(
        alias = "LFCES038",
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
    'COD_MUN',
    'COD_AREA',
    'SEQ_EQUIPE',
    'PROF_ID',
    'UNIDADE_ID',
    'COD_CBO',
    'TP_SUS_NAO_SUS',
    'IND_VINC',
    'MICROAREA',
    'DT_ENTRADA',
    'DT_DESLIGAMENTO',
    'CNES_OUTRAEQUIPE',
    'COD_MUN_OUTRAEQUIPE',
    'COD_AREA_OUTRAEQUIPE',
    'PROF_ID_CH_COMPL',
    'COD_CBO_CH_COMPL',
    'FL_EQUIPEMINIMA',
    'CO_MUN_ATUACAO',
    'DATA_ATU',
    'USUARIO',
    'DT_ATUALIZACAO_ORIGEM',
    'DT_CMTP_INICIO',
    'DT_CMTP_FIM',
    'NU_SEQ_PROCESSO'
] %}

with source as (
    select *
    from {{ source("brutos_gdb_cnes_staging", "LFCES038") }}
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
