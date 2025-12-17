{{
    config(
        alias="equipe",
        schema= "brutos_gdb_cnes"
    )
}}

with source as (
    select * from {{ source("brutos_gdb_cnes_staging", "LFCES037") }}
),
extracted as (
    select
        json_extract_scalar(json, "$.TP_EQUIPE") as TP_EQUIPE,
        json_extract_scalar(json, "$.UNIDADE_ID") as UNIDADE_ID,
        json_extract_scalar(json, "$.COD_MUN") as COD_MUN,
        json_extract_scalar(json, "$.COD_AREA") as COD_AREA,
        json_extract_scalar(json, "$.CD_MOTIVO_DESATIV") as CD_MOTIVO_DESATIV,
        json_extract_scalar(json, "$.CD_TP_DESATIV") as CD_TP_DESATIV,
        json_extract_scalar(json, "$.CO_EQUIPE") as CO_EQUIPE,
        json_extract_scalar(json, "$.SEQ_EQUIPE") as SEQ_EQUIPE,
        json_extract_scalar(json, "$.NM_REFERENCIA") as NM_REFERENCIA,
        json_extract_scalar(json, "$.CO_SUB_TIPO_EQUIPE") as CO_SUB_TIPO_EQUIPE,
        json_extract_scalar(json, "$.DT_ATIVACAO") as DT_ATIVACAO,
        json_extract_scalar(json, "$.DT_DESATIVACAO") as DT_DESATIVACAO,
        json_extract_scalar(json, "$.TP_POP_ASSIST_QUILOMB") as TP_POP_ASSIST_QUILOMB,
        json_extract_scalar(json, "$.TP_POP_ASSIST_ASSENT") as TP_POP_ASSIST_ASSENT,
        json_extract_scalar(json, "$.TP_POP_ASSIST_GERAL") as TP_POP_ASSIST_GERAL,
        json_extract_scalar(json, "$.TP_POP_ASSIST_ESCOLA") as TP_POP_ASSIST_ESCOLA,
        json_extract_scalar(json, "$.TP_POP_ASSIST_PRONASCI") as TP_POP_ASSIST_PRONASCI,
        json_extract_scalar(json, "$.TP_POP_ASSIST_INDIGENA") as TP_POP_ASSIST_INDIGENA,
        json_extract_scalar(json, "$.TP_POP_ASSIST_RIBEIRINHA") as TP_POP_ASSIST_RIBEIRINHA,
        json_extract_scalar(json, "$.TP_POP_ASSIST_SITUACAO_RUA") as TP_POP_ASSIST_SITUACAO_RUA,
        json_extract_scalar(json, "$.TP_POP_ASSIST_PRIV_LIBERDADE") as TP_POP_ASSIST_PRIV_LIBERDADE,
        json_extract_scalar(json, "$.TP_POP_ASSIST_CONFLITO_LEI") as TP_POP_ASSIST_CONFLITO_LEI,
        json_extract_scalar(json, "$.TP_POP_ASSIST_ADOL_CONF_LEI") as TP_POP_ASSIST_ADOL_CONF_LEI,
        json_extract_scalar(json, "$.CO_CNES_UOM") as CO_CNES_UOM,
        json_extract_scalar(json, "$.NU_CH_AMB_UOM") as NU_CH_AMB_UOM,
        json_extract_scalar(json, "$.CO_PROF_SUS_PRECEPTOR") as CO_PROF_SUS_PRECEPTOR,
        json_extract_scalar(json, "$.CO_CNES_PRECEPTOR") as CO_CNES_PRECEPTOR,
        json_extract_scalar(json, "$.DATA_ATU") as DATA_ATU,
        json_extract_scalar(json, "$.USUARIO") as USUARIO,
        _source_file,
        _loaded_at,
        data_particao
    from source
),

renamed as (
    select
        -- TP_EQUIPE: FK NFCES046
        cast({{ process_null("TP_EQUIPE") }} as string) as id_equipe_tipo,
        -- UNIDADE_ID: FK LFCES004
        cast({{ process_null("UNIDADE_ID") }} as string) as id_unidade,
        -- COD_MUN: FK LFCES041
        cast({{ process_null("COD_MUN") }} as string) as id_municipio,
        -- COD_AREA: FK LFCES041
        cast({{ process_null("COD_AREA") }} as string) as id_area,
        -- CD_MOTIVO_DESATIV: FK NFCES053
        cast({{ process_null("CD_MOTIVO_DESATIV") }} as string) as id_motivacao_desativacao_equipe,
        -- CD_TP_DESATIV: FK NFCES050
        cast({{ process_null("CD_TP_DESATIV") }} as string) as id_tipo_desativacao_equipe,
        case
            when trim(CD_TP_DESATIV)="01" then "Temporária"
            when trim(CD_TP_DESATIV)="02" then "Definitiva"
            else null
        end as tipo_desativacao_equipe,

        cast({{ process_null("CO_EQUIPE") }} as string) as equipe_ine,
        cast({{ process_null("SEQ_EQUIPE") }} as string) as equipe_sequencial,
        cast({{ process_null("NM_REFERENCIA") }} as string) as equipe_nome,
        cast({{ process_null("CO_SUB_TIPO_EQUIPE") }} as string) as id_subtipo_equipe,
        safe_cast({{ process_null("DT_ATIVACAO") }} as date) as data_ativacao,
        safe_cast({{ process_null("DT_DESATIVACAO") }} as date) as data_desativacao,
        cast({{ process_null("TP_POP_ASSIST_QUILOMB") }} as string) as atende_pop_quilombola,
        cast({{ process_null("TP_POP_ASSIST_ASSENT") }} as string) as atende_pop_assentados,
        cast({{ process_null("TP_POP_ASSIST_GERAL") }} as string) as atende_pop_geral,
        cast({{ process_null("TP_POP_ASSIST_ESCOLA") }} as string) as atende_pop_escola,
        cast({{ process_null("TP_POP_ASSIST_PRONASCI") }} as string) as atende_pop_pronasci,
        cast({{ process_null("TP_POP_ASSIST_INDIGENA") }} as string) as atende_pop_indigena,
        cast({{ process_null("TP_POP_ASSIST_RIBEIRINHA") }} as string) as atende_pop_ribeirinha,
        cast({{ process_null("TP_POP_ASSIST_SITUACAO_RUA") }} as string) as atende_pop_situacao_rua,
        cast({{ process_null("TP_POP_ASSIST_PRIV_LIBERDADE") }} as string) as atende_pop_privada_liberdade,
        cast({{ process_null("TP_POP_ASSIST_CONFLITO_LEI") }} as string) as atende_pop_conflito_lei,
        cast({{ process_null("TP_POP_ASSIST_ADOL_CONF_LEI") }} as string) as atende_pop_adolescente_conflito_lei,
        cast({{ process_null("CO_CNES_UOM") }} as string) as id_cnes_uom,
        cast({{ process_null("NU_CH_AMB_UOM") }} as string) as carga_horaria_uom,
        cast({{ process_null("CO_PROF_SUS_PRECEPTOR") }} as string) as id_profissional_preceptor,
        cast({{ process_null("CO_CNES_PRECEPTOR") }} as string) as id_cnes_preceptor,
        safe_cast({{ process_null("DATA_ATU") }} as date) as data_atualizacao,
        cast({{ process_null("USUARIO") }} as string) as usuario,

        -- Podem ser usados posteriormente para deduplicação
        data_particao,
        _loaded_at as data_carga,
    from extracted
)
select *
from renamed
