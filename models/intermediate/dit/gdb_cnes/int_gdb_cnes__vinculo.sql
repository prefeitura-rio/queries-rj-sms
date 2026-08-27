{{
    config(
        schema="intermediario_gdb_cnes",
        alias="vinculo",
        materialized="table",
        tags=["gdb_cnes"]
    )
}}


with
    vinculo as (
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

            data_particao as competencia,
            _loaded_at

        from {{ ref("raw_gdb_cnes__lfces021") }}
        where data_particao = (
            select max(data_particao)
            from {{ ref("raw_gdb_cnes__lfces021") }}
        )
        -- Às vezes temos múltiplos GDBs pra uma mesma competência,
        -- porque são revisados etc; então precisamos deduplicar
        qualify row_number() over (
            partition by id_profissional_sus, id_unidade, id_cbo
            order by _loaded_at desc
        ) = 1
    )

select *
from vinculo
