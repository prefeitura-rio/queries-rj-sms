{{
    config(
        alias="estabelecimento",
        schema= "brutos_gdb_cnes"
    )
}}

with source as (
    select * from {{ source('brutos_gdb_cnes_staging', 'LFCES004') }}
),
extracted as (
    select
        json_extract_scalar(json, "$.TP_UNID_ID") as TP_UNID_ID,
        json_extract_scalar(json, "$.CO_TIPO_ESTABELECIMENTO") as CO_TIPO_ESTABELECIMENTO,
        json_extract_scalar(json, "$.CO_ATIVIDADE_PRINCIPAL") as CO_ATIVIDADE_PRINCIPAL,
        json_extract_scalar(json, "$.CODMUNGEST") as CODMUNGEST,
        json_extract_scalar(json, "$.SIGESTGEST") as SIGESTGEST,
        json_extract_scalar(json, "$.REG_SAUDE") as REG_SAUDE,
        json_extract_scalar(json, "$.COD_ATIV") as COD_ATIV,
        json_extract_scalar(json, "$.COD_CLIENT") as COD_CLIENT,
        json_extract_scalar(json, "$.COD_TURNAT") as COD_TURNAT,
        json_extract_scalar(json, "$.CD_MOTIVO_DESAB") as CD_MOTIVO_DESAB,
        json_extract_scalar(json, "$.CO_NATUREZA_JUR") as CO_NATUREZA_JUR,
        json_extract_scalar(json, "$.UNIDADE_ID") as UNIDADE_ID,
        json_extract_scalar(json, "$.CNES") as CNES,
        json_extract_scalar(json, "$.MICRO_REG") as MICRO_REG,
        json_extract_scalar(json, "$.DIST_SANIT") as DIST_SANIT,
        json_extract_scalar(json, "$.DIST_ADMIN") as DIST_ADMIN,
        json_extract_scalar(json, "$.CNPJ_MANT") as CNPJ_MANT,
        json_extract_scalar(json, "$.PFPJ_IND") as PFPJ_IND,
        json_extract_scalar(json, "$.NIVEL_DEP") as NIVEL_DEP,
        json_extract_scalar(json, "$.ST_CONTRATO_FORMALIZADO") as ST_CONTRATO_FORMALIZADO,
        json_extract_scalar(json, "$.R_SOCIAL") as R_SOCIAL,
        json_extract_scalar(json, "$.NOME_FANTA") as NOME_FANTA,
        json_extract_scalar(json, "$.LOGRADOURO") as LOGRADOURO,
        json_extract_scalar(json, "$.NUMERO") as NUMERO,
        json_extract_scalar(json, "$.COMPLEMENT") as COMPLEMENT,
        json_extract_scalar(json, "$.BAIRRO") as BAIRRO,
        json_extract_scalar(json, "$.COD_CEP") as COD_CEP,
        json_extract_scalar(json, "$.NU_LATITUDE") as NU_LATITUDE,
        json_extract_scalar(json, "$.NU_LONGITUDE") as NU_LONGITUDE,
        json_extract_scalar(json, "$.TELEFONE") as TELEFONE,
        json_extract_scalar(json, "$.FAX") as FAX,
        json_extract_scalar(json, "$.E_MAIL") as E_MAIL,
        json_extract_scalar(json, "$.NO_URL") as NO_URL,
        json_extract_scalar(json, "$.CPF") as CPF,
        json_extract_scalar(json, "$.CNPJ") as CNPJ,
        json_extract_scalar(json, "$.TP_ESTAB_SEMPRE_ABERTO") as TP_ESTAB_SEMPRE_ABERTO,
        json_extract_scalar(json, "$.ST_CONEXAOINTERNET") as ST_CONEXAOINTERNET,
        json_extract_scalar(json, "$.NUM_ALVARA") as NUM_ALVARA,
        json_extract_scalar(json, "$.DATA_EXPED") as DATA_EXPED,
        json_extract_scalar(json, "$.IND_ORGEXP") as IND_ORGEXP,
        json_extract_scalar(json, "$.DT_VAL_LIC_SANI") as DT_VAL_LIC_SANI,
        json_extract_scalar(json, "$.TP_LIC_SANI") as TP_LIC_SANI,
        json_extract_scalar(json, "$.CPFDIRETORCLINICO") as CPFDIRETORCLINICO,
        json_extract_scalar(json, "$.REGDIRETORCLINICO") as REGDIRETORCLINICO,
        json_extract_scalar(json, "$.FL_ADESAO_FILANTROP") as FL_ADESAO_FILANTROP,
        json_extract_scalar(json, "$.DATA_ATU") as DATA_ATU,
        json_extract_scalar(json, "$.USUARIO") as USUARIO,
        json_extract_scalar(json, "$.DT_ATU_GEO") as DT_ATU_GEO,
        json_extract_scalar(json, "$.NO_USUARIO_GEO") as NO_USUARIO_GEO,
        json_extract_scalar(json, "$.ST_GERACREDITO_GERENTE_SGIF") as ST_GERACREDITO_GERENTE_SGIF,
        _source_file,
        _loaded_at,
        data_particao
    from source
),

renamed as (
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

        -- Podem ser usados posteriormente para deduplicação
        data_particao,
        _loaded_at as data_carga
    from extracted
)

select *
from renamed
