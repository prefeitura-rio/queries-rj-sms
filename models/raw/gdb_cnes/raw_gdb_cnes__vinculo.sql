{{
    config(
        alias="vinculo",
        schema= "brutos_gdb_cnes"
    )
}}

with source as (
    select * from {{ source('brutos_gdb_cnes_staging', 'LFCES021') }}
),
extracted as (
    select
        json_extract_scalar(json, "$.UNIDADE_ID") as UNIDADE_ID,
        json_extract_scalar(json, "$.PROF_ID") as PROF_ID,
        json_extract_scalar(json, "$.COD_CBO") as COD_CBO,
        json_extract_scalar(json, "$.IND_VINC") as IND_VINC,
        json_extract_scalar(json, "$.CONSELHOID") as CONSELHOID,
        json_extract_scalar(json, "$.TP_SUS_NAO_SUS") as TP_SUS_NAO_SUS,
        json_extract_scalar(json, "$.NU_CNPJ_DET_VINC") as NU_CNPJ_DET_VINC,
        json_extract_scalar(json, "$.CGHORAOUTR") as CGHORAOUTR,
        json_extract_scalar(json, "$.CG_HORAAMB") as CG_HORAAMB,
        json_extract_scalar(json, "$.CGHORAHOSP") as CGHORAHOSP,
        json_extract_scalar(json, "$.N_REGISTRO") as N_REGISTRO,
        json_extract_scalar(json, "$.SG_UF_CRM") as SG_UF_CRM,
        json_extract_scalar(json, "$.TP_PRECEPTOR") as TP_PRECEPTOR,
        json_extract_scalar(json, "$.TP_RESIDENTE") as TP_RESIDENTE,
        json_extract_scalar(json, "$.STATUSMOV") as STATUSMOV,
        json_extract_scalar(json, "$.DATA_ATU") as DATA_ATU,
        json_extract_scalar(json, "$.USUARIO") as USUARIO,
        _source_file,
        _loaded_at,
        data_particao
    from source
),

renamed as (
    select
        substr(upper(to_hex(md5(cast(PROF_ID as string)))), 0, 16) as id_profissional_sus,

        -- UNIDADE_ID: FK LFCES004
        cast({{ process_null("UNIDADE_ID") }} as string) as id_unidade,
        -- PROF_ID: FK LFCES018
        cast({{ process_null("PROF_ID") }} as string) as id_profissional_cnes,
        -- COD_CBO: FK NFCES026
        cast({{ process_null("COD_CBO") }} as string) as id_cbo,
        -- IND_VINC: FK NFCES058
        cast({{ process_null("IND_VINC") }} as string) as id_vinculo,
        -- CONSELHOID: FK NFCES033
        cast({{ process_null("CONSELHOID") }} as string) as id_conselho,

        if(lower(trim(TP_SUS_NAO_SUS))='s',true,false) as atende_sus,
        cast({{ process_null("NU_CNPJ_DET_VINC") }} as string) as empregador_cnpj,
        cast({{ process_null("CGHORAOUTR") }} as integer) as carga_horaria_outros,
        cast({{ process_null("CG_HORAAMB") }} as integer) as carga_horaria_ambulatorial,
        cast({{ process_null("CGHORAHOSP") }} as integer) as carga_horaria_hospitalar,
        cast({{ process_null("N_REGISTRO") }} as string) as conselho_numero_registro,
        cast({{ process_null("SG_UF_CRM") }} as string) as uf_crm,
        if(trim(TP_PRECEPTOR)='1',true,false) as eh_preceptor,
        if(trim(TP_RESIDENTE)='1',true,false) as eh_residente,
        case
            when trim(STATUSMOV)='1' then 'Não aprovado'
            when trim(STATUSMOV)='2' then 'Consistido'
            when trim(STATUSMOV)='3' then 'Exportado'
            else null
        end as status_vinculo,
        cast({{ process_null("DATA_ATU") }} as date) as data_ultima_atualizacao,
        cast({{ process_null("USUARIO") }} as string) as usuario_atualizador,

        -- Podem ser usados posteriormente para deduplicação
        data_particao,
        _loaded_at as data_carga,
    from extracted
)
select *
from renamed
