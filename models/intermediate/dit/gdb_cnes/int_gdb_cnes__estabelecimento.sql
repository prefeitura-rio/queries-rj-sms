{{
    config(
        schema="intermediario_gdb_cnes",
        alias="estabelecimento",
        materialized="table",
        tags=["gdb_cnes"]
    )
}}

with
    base as (
        select
            -- TP_UNID_ID: FK NFCES010
            cast({{ process_null("TP_UNID_ID") }} as string) as id_tipo_unidade,
            -- CO_TIPO_ESTABELECIMENTO: FK NFCES119
            cast({{ process_null("CO_TIPO_ESTABELECIMENTO") }} as string) as id_tipo_estabelecimento,
            -- CO_ATIVIDADE_PRINCIPAL: FK NFCES118
            cast({{ process_null("CO_ATIVIDADE_PRINCIPAL") }} as string) as id_atividade_principal,
            -- CODMUNGEST: FK NFCES005
            cast({{ process_null("CODMUNGEST") }} as string) as id_municipio_gestor,
            -- SIGESTGEST: FK NFCES013
            cast({{ process_null("SIGESTGEST") }} as string) as estado_gestor_sigla,
            -- REG_SAUDE: FK LFCES029
            cast({{ process_null("REG_SAUDE") }} as string) as id_regiao_saude,
            -- COD_ATIV: FK NFCES007
            cast({{ process_null("COD_ATIV") }} as string) as id_atividade,
            -- COD_CLIENT: FK NFCES002
            cast({{ process_null("COD_CLIENT") }} as string) as id_cliente,
            -- COD_TURNAT: FK NFCES011
            cast({{ process_null("COD_TURNAT") }} as string) as id_turno_atendimento,
            --CD_MOTIVO_DESAB: FK NFCES049
            cast({{ process_null("CD_MOTIVO_DESAB") }} as string) as id_motivo_desabilitacao,
            -- CO_NATUREZA_JUR: FK NFCES085
            cast({{ process_null("CO_NATUREZA_JUR") }} as string) as id_natureza_juridica,

            cast({{ process_null("UNIDADE_ID") }} as string) as id_unidade,
            cast({{ process_null("CNES") }} as string) as id_cnes,
            cast({{ process_null("MICRO_REG") }} as string) as id_micro_regiao,
            cast({{ process_null("DIST_SANIT") }} as string) as id_distrito_sanitario,
            cast({{ process_null("DIST_ADMIN") }} as string) as id_distrito_administrativo,
            cast({{ process_null("CNPJ_MANT") }} as string) as cnpj_mantenedora,
            case
                when trim(PFPJ_IND)='1' then 'Pessoa física'
                when trim(PFPJ_IND)='3' then 'Pessoa jurídica'
                else null
            end as tipo_pessoa,
            case
                when trim(NIVEL_DEP)='1' then 'Individual'
                when trim(NIVEL_DEP)='3' then 'Mantido'
                else null
            end as dependencia_nivel,
            case 
                when lower(trim(ST_CONTRATO_FORMALIZADO))='s' then true
                when lower(trim(ST_CONTRATO_FORMALIZADO))='n' then false
                else null
            end as contrato_formalizado_sus,
            cast({{ process_null("R_SOCIAL") }} as string) as nome_razao_social,
            cast({{ process_null("NOME_FANTA") }} as string) as nome_fantasia,
            cast({{ process_null("LOGRADOURO") }} as string) as endereco_logradouro,
            cast({{ process_null("NUMERO") }} as string) as endereco_numero,
            cast({{ process_null("COMPLEMENT") }} as string) as endereco_complemento,
            cast({{ process_null("BAIRRO") }} as string) as endereco_bairro,
            cast({{ process_null("COD_CEP") }} as string) as endereco_cep,
            cast({{ process_null("NU_LATITUDE") }} as string) as endereco_latitude,
            cast({{ process_null("NU_LONGITUDE") }} as string) as endereco_longitude,
            cast({{ process_null("TELEFONE") }} as string) as telefone,
            cast({{ process_null("FAX") }} as string) as fax,
            cast({{ process_null("E_MAIL") }} as string) as email,
            cast({{ process_null("NO_URL") }} as string) as url,
            cast({{ process_null("CPF") }} as string) as cpf,
            cast({{ process_null("CNPJ") }} as string) as cnpj,
            case 
                when lower(trim(TP_ESTAB_SEMPRE_ABERTO))='s' then true
                when lower(trim(TP_ESTAB_SEMPRE_ABERTO))='n' then false
                else null
            end as aberto_sempre,
            case
                when lower(trim(ST_CONEXAOINTERNET))='s' then true
                when lower(trim(ST_CONEXAOINTERNET))='n' then false
                else null
            end as possui_conexao_internet,
            cast({{ process_null("NUM_ALVARA") }} as string) as alvara_numero,
            safe_cast({{ process_null("DATA_EXPED") }} as date) as alvara_data_expedicao,
            case
                when trim(IND_ORGEXP)='1' then 'SES'
                when trim(IND_ORGEXP)='2' then 'SMS'
                else null
            end as alvara_orgao_expedidor,
            safe_cast({{ process_null("DT_VAL_LIC_SANI") }} as date) as licenca_sanitaria_data_validade,
            case
                when trim(TP_LIC_SANI)='1' then 'Total'
                when trim(TP_LIC_SANI)='2' then 'Parcial/Restrições'
                else null
            end as licenca_sanitaria_tipo,
            cast({{ process_null("CPFDIRETORCLINICO") }} as string) as diretor_clinico_cpf,
            cast({{ process_null("REGDIRETORCLINICO") }} as string) as diretor_clinico_conselho,
            case
                when trim(FL_ADESAO_FILANTROP) = '1' then true
                when trim(FL_ADESAO_FILANTROP) = '2' then false
                else null
            end as adesao_hospital_filantropico,
            safe_cast({{ process_null("DATA_ATU") }} as date) as data_atualizacao_registro,
            cast({{ process_null("USUARIO") }} as string) as usuario_atualizador_registro,
            safe_cast({{ process_null("DT_ATU_GEO") }} as date) as data_atualizacao_geolocalizacao,
            cast({{ process_null("NO_USUARIO_GEO") }} as string) as usuario_atualizador_geolocalizacao,
            case
                when lower(trim(ST_GERACREDITO_GERENTE_SGIF))='s' then true
                when lower(trim(ST_GERACREDITO_GERENTE_SGIF))='n' then false
                else null
            end as gera_credito_gerente_sgif,

            data_particao as competencia,
            _loaded_at

        from {{ ref("raw_gdb_cnes__lfces004") }}
        where data_particao = (
            select max(data_particao)
            from {{ ref("raw_gdb_cnes__lfces004") }}
        )
        qualify row_number() over (
            partition by id_unidade
            order by _loaded_at desc
        ) = 1
    ),

    final as (
        select
            base.*,

            regexp_extract(
                upper(trim(nome_fantasia)),
                r'^(SMS(?:\s+RIO)?|SES\s+RJ|MS|UFRJ|UERJ\s+HUPE|FIOTEC\s+IFF)\b'
            ) as sigla,

            nullif(
                {{ remove_duplicate_whitespace(
                    "trim(regexp_replace(regexp_replace(upper(trim(nome_fantasia)), r'^(SMS(?:\\s+RIO)?|SES\\s+RJ|MS|UFRJ|UERJ\\s+HUPE|FIOTEC\\s+IFF)\\b[\\s\\-–:/]*', ''), r'\\s*-?\\s*AP\\s*\\d{1,2}$', ''))"
                ) }},
                ''
            ) as nome_limpo,

            nullif(trim(id_distrito_sanitario), '') as area_programatica

        from base
    )

select *
from final
