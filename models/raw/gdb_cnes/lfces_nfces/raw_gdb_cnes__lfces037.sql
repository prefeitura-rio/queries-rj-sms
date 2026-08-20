{{
    config(
        alias = "LFCES037",
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
    'UNIDADE_ID',
    'TP_EQUIPE',
    'CO_SUB_TIPO_EQUIPE',
    'NM_REFERENCIA',
    'DT_ATIVACAO',
    'DT_DESATIVACAO',
    'TP_POP_ASSIST_QUILOMB',
    'TP_POP_ASSIST_ASSENT',
    'TP_POP_ASSIST_GERAL',
    'TP_POP_ASSIST_ESCOLA',
    'TP_POP_ASSIST_PRONASCI',
    'TP_POP_ASSIST_INDIGENA',
    'TP_POP_ASSIST_RIBEIRINHA',
    'TP_POP_ASSIST_SITUACAO_RUA',
    'TP_POP_ASSIST_PRIV_LIBERDADE',
    'TP_POP_ASSIST_CONFLITO_LEI',
    'TP_POP_ASSIST_ADOL_CONF_LEI',
    'CO_CNES_UOM',
    'NU_CH_AMB_UOM',
    'CD_MOTIVO_DESATIV',
    'CD_TP_DESATIV',
    'CO_PROF_SUS_PRECEPTOR',
    'CO_CNES_PRECEPTOR',
    'CO_EQUIPE',
    'DATA_ATU',
    'USUARIO',
    'STATUSMOV',
    'DT_ATUALIZACAO_ORIGEM',
    'DT_CMTP_INICIO',
    'DT_CMTP_FIM',
    'NU_SEQ_PROCESSO'
] %}

with source as (
    select *
    from {{ source("brutos_gdb_cnes_staging", "LFCES037") }}
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
