{{
    config(
        alias="equipe",
        schema= "brutos_cnes_gdb"
    )
}}

with 
    source as (
        select * from {{ source("brutos_cnes_gdb_staging", "equipe") }}
    )

select     
    cast(CO_EQUIPE as string) as equipe_ine,
    cast(SEQ_EQUIPE as string) as equipe_sequencial,
    cast(NM_REFERENCIA as string) as equipe_nome_referencia,
    cast(TP_EQUIPE as string) as id_equipe_tipo,
    cast(CO_SUB_TIPO_EQUIPE as string) as id_equipe_subtipo,
    cast(UNIDADE_ID as string) as id_unidade,
    cast(COD_MUN as string) as municipio_codigo,
    cast(COD_AREA as string) as id_area,
    cast(DT_ATIVACAO as date) as data_ativacao,
    cast(DT_DESATIVACAO as date) as data_desativacao,
    cast(TP_POP_ASSIST_QUILOMB as string) as atende_pop_quilombola,
    cast(TP_POP_ASSIST_ASSENT as string) as atende_pop_assentados,
    cast(TP_POP_ASSIST_GERAL as string) as atende_pop_geral,
    cast(TP_POP_ASSIST_ESCOLA as string) as atende_pop_escola,
    cast(TP_POP_ASSIST_PRONASCI as string) as atende_pop_pronasci,
    cast(TP_POP_ASSIST_INDIGENA as string) as atende_pop_indigena,
    cast(TP_POP_ASSIST_RIBEIRINHA as string) as atende_pop_ribeirinha,
    cast(TP_POP_ASSIST_SITUACAO_RUA as string) as atende_pop_situacao_rua,
    cast(TP_POP_ASSIST_PRIV_LIBERDADE as string) as atende_pop_privada_liberdade,
    cast(TP_POP_ASSIST_CONFLITO_LEI as string) as atende_pop_conflito_lei,
    cast(TP_POP_ASSIST_ADOL_CONF_LEI as string
    ) as atende_pop_adolescente_conflito_lei,
    cast(CD_MOTIVO_DESATIV as string) as id_motivacao_desativacao_equipe,
    cast(CD_TP_DESATIV as string) as id_tipo_desativacao_equipe,
    cast(CO_CNES_UOM as string) as id_cnes_uom,
    cast(NU_CH_AMB_UOM as string) as carga_horaria_uom,
    cast(CO_PROF_SUS_PRECEPTOR as string) as id_profissional_preceptor,
    cast(CO_CNES_PRECEPTOR as string) as id_cnes_preceptor,
    cast(DATA_ATU as date) as data_atualizacao,
    cast(USUARIO as string) as usuario
from source