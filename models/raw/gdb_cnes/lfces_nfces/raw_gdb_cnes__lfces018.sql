{{
    config(
        alias = "LFCES018",
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
    'PROF_ID',
    'CPF_PROF',
    'PISPASEP',
    'NOME_PROF',
    'NOME_MAE',
    'DATA_NASC',
    'COD_MUN',
    'SEXO',
    'NUM_LIVRO',
    'NUM_FOLHA',
    'NUM_TERMO',
    'CODORGEMIS',
    'DATA_EMISS',
    'NUM_IDENT',
    'SIGLA_EST',
    'DTEMIIDENT',
    'DATA_ENTRA',
    'CTPS_NUMER',
    'SERIE',
    'SIGESTCTPS',
    'DTEMISCTPS',
    'LOGRADOURO',
    'NUMERO',
    'COMPLEMENT',
    'BAIRRODIST',
    'COD_CEP',
    'SIGLA_UF',
    'CODESCOLAR',
    'COD_CERTID',
    'IND_NACIO',
    'NOME_CARTO',
    'COD_BANCO',
    'NUM_AGENC',
    'CONTA_CC',
    'NOME_PAIS',
    'COD_CNS',
    'D_TERCSIH',
    'STATUS',
    'STATUSMOV',
    'DATA_ATU',
    'USUARIO',
    'NMUSUARIOEMUSO',
    'CD_RACA',
    'NOME_PAI',
    'TELEFONE',
    'CD_TP_LOGR',
    'PORTARIA',
    'DT_NATUR',
    'CD_PAIS',
    'COD_MUN_RES',
    'UF_RES',
    'CHKSUM',
    'CO_ETNIA',
    'NO_EMAIL',
    'ST_NMPROF_CADSUS',
    'CO_PAIS_RESID',
    'NU_CARTEIRA_HAB',
    'DT_EMIS_CARTEIRA_HAB',
    'UF_CARTEIRA_HAB',
    'CO_NACIONALIDADE',
    'CO_SEQ_INCLUSAO',
    'DT_ATUALIZACAO_ORIGEM',
    'DT_CMTP_INICIO',
    'DT_CMTP_FIM',
    'NU_SEQ_PROCESSO',
    'NO_SOCIAL'
] %}

with source as (
    select *
    from {{ source("brutos_gdb_cnes_staging", "LFCES018") }}
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
