{{
    config(
        alias="vinculo",
        schema= "brutos_gdb_cnes"
    )
}}

with source as (
    select * from {{ source('brutos_gdb_cnes_staging', 'LFCES021') }}
),
renamed as (
    select
        substr(upper(to_hex(md5(cast(PROF_ID as string)))), 0, 16) as id_profissional_sus,

        -- UNIDADE_ID: FK LFCES004
        cast(UNIDADE_ID as string) as id_unidade,
        -- PROF_ID: FK LFCES018
        cast(PROF_ID as string) as id_profissional_cnes,
        -- COD_CBO: FK NFCES026
        cast(COD_CBO as string) as id_cbo,
        -- IND_VINC: FK NFCES058
        cast(IND_VINC as string) as id_vinculo,
        -- CONSELHOID: FK NFCES033
        cast(CONSELHOID as string) as id_conselho,

        if(lower(trim(TP_SUS_NAO_SUS))='s',true,false) as atende_sus,
        cast(NU_CNPJ_DET_VINC as string) as empregador_cnpj,
        cast(CGHORAOUTR as integer) as carga_horaria_outros,
        cast(CG_HORAAMB as integer) as carga_horaria_ambulatorial,
        cast(CGHORAHOSP as integer) as carga_horaria_hospitalar,
        cast(N_REGISTRO as string) as conselho_numero_registro,
        cast(SG_UF_CRM as string) as uf_crm,
        if(trim(TP_PRECEPTOR)='1',true,false) as eh_preceptor,
        if(trim(TP_RESIDENTE)='1',true,false) as eh_residente,
        case
            when trim(STATUSMOV)='1' then 'Não aprovado'
            when trim(STATUSMOV)='2' then 'Consistido'
            when trim(STATUSMOV)='3' then 'Exportado'
            else null
        end as status_vinculo,
        cast(DATA_ATU as date) as data_ultima_atualizacao,
        cast(USUARIO as string) as usuario_atualizador,

        -- Podem ser usados posteriormente para deduplicação
        data_particao,
        _loaded_at as data_carga,
    from source
)
select *
from renamed
