{{
    config(
        alias = "LFCES004",
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
    'CNES',
    'CNPJ_MANT',
    'PFPJ_IND',
    'NIVEL_DEP',
    'R_SOCIAL',
    'NOME_FANTA',
    'LOGRADOURO',
    'NUMERO',
    'COMPLEMENT',
    'BAIRRO',
    'COD_CEP',
    'REG_SAUDE',
    'MICRO_REG',
    'DIST_SANIT',
    'DIST_AMIN',
    'TELEFONE',
    'FAX',
    'E_MAIL',
    'CPF',
    'CNPJ',
    'COD_ATIV',
    'COD_CLIENT',
    'NUM_ALVARA',
    'DATA_EXPED',
    'IND_ORGEXP',
    'DT_VAL_LIC_SANI',
    'TP_LIC_SANI',
    'TP_UNID_ID',
    'COD_TURNAT',
    'SIGESTGEST',
    'CODMUNGEST',
    'STATUS',
    'STATUSMOV',
    'DATA_ATU',
    'USUARIO',
    'NMUSUARIOEMUSO',
    'CPFDIRETORCLINICO',
    'REGDIRETORCLINICO',
    'FL_ADESAO_FILANTROP',
    'CD_MOTIVO_DESAB',
    'NO_URL',
    'NU_LATITUDE',
    'NU_LONGITUDE',
    'DT_ATU_GEO',
    'NO_USUARIO_GEO',
    'CO_NATUREZA_JUR',
    'TP_ESTAB_SEMPRE_ABERTO',
    'ST_GERACREDITO_GERENTE_SGIF',
    'ST_NAT_JUR_WEBSERVICE',
    'ST_DADOS_CADONLINE_WEBSERV',
    'ST_CONEXAOINTERNET',
    'CHKSUM',
    'DT_VALIDACAO',
    'CO_TIPO_ESTABELECIMENTO',
    'CO_ATIVIDADE_PRINCIPAL',
    'ST_CONTRATO_FORMALIZADO',
    'CO_TIPO_UNIDADE',
    'NO_FANTASIA_ABREV',
    'TP_GESTAO',
    'DT_ATUALIZACAO_ORIGEM',
    'DT_CMTP_INICIO',
    'DT_CMTP_FIM',
    'CO_TIPO_ABRANGENCIA',
    'ST_COWORKING'
] %}

with source as (
    select *
    from {{ source("brutos_gdb_cnes_staging", "LFCES004") }}
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
