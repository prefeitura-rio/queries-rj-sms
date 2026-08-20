{{
    config(
        alias = "LFCES021",
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
    'UNIDADE_ID',
    'PROF_ID',
    'COD_CBO',
    'TP_SUS_NAO_SUS',
    'IND_VINC',
    'D_TERCSIH',
    'CG_HORAAMB',
    'CGHORAHOSP',
    'CGHORAOUTR',
    'CONSELHOID',
    'N_REGISTRO',
    'SG_UF_CRM',
    'STATUS',
    'STATUSMOV',
    'TP_PRECEPTOR',
    'TP_RESIDENTE',
    'NU_CNPJ_DET_VINC',
    'DATA_ATU',
    'USUARIO',
    'CHKSUM',
    'DT_ATUALIZACAO_ORIGEM',
    'DT_CMTP_INICIO',
    'DT_CMTP_FIM',
    'NU_SEQ_PROCESSO'
] %}

with source as (
    select *
    from {{ source("brutos_gdb_cnes_staging", "LFCES021") }}
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
