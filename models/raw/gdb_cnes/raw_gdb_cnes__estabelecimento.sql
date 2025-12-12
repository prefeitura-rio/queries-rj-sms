{{
    config(
        alias="estabelecimento",
        schema= "brutos_gdb_cnes"
    )
}}

with source as (
    select * from {{ source('brutos_gdb_cnes_staging', 'LFCES004') }}
),
renamed as (
    select
        -- TP_UNID_ID: FK NFCES010
        cast(TP_UNID_ID as string) as id_tipo_unidade,
        -- CO_TIPO_ESTABELECIMENTO: FK NFCES119
        cast(CO_TIPO_ESTABELECIMENTO as string) as id_tipo_estabelecimento,
        -- CO_ATIVIDADE_PRINCIPAL: FK NFCES118
        cast(CO_ATIVIDADE_PRINCIPAL as string) as id_atividade_principal,
        -- CODMUNGEST: FK NFCES005
        cast(CODMUNGEST as string) as id_municipio_gestor,
        -- SIGESTGEST: FK NFCES013
        cast(SIGESTGEST as string) as estado_gestor_sigla,
        -- REG_SAUDE: FK LFCES029
        cast(REG_SAUDE as string) as id_regiao_saude,
        -- COD_ATIV: FK NFCES007
        cast(COD_ATIV as string) as id_atividade,
        -- COD_CLIENT: FK NFCES002
        cast(COD_CLIENT as string) as id_cliente,
        -- COD_TURNAT: FK NFCES011
        cast(COD_TURNAT as string) as id_turno_atendimento,
        --CD_MOTIVO_DESAB: FK NFCES049
        cast(CD_MOTIVO_DESAB as string) as id_motivo_desabilitacao,
        -- CO_NATUREZA_JUR: FK NFCES085
        cast(CO_NATUREZA_JUR as string) as id_natureza_juridica,

        cast(UNIDADE_ID as string) as id_unidade,
        cast(CNES as string) as id_cnes,
        cast(MICRO_REG as string) as id_micro_regiao,
        cast(DIST_SANIT as string) as id_distrito_sanitario,
        cast(DIST_ADMIN as string) as id_distrito_administrativo,
        cast(CNPJ_MANT as string) as cnpj_mantenedora,
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
        cast(R_SOCIAL as string) as nome_razao_social,
        cast(NOME_FANTA as string) as nome_fantasia,
        cast(LOGRADOURO as string) as endereco_logradouro,
        cast(NUMERO as string) as endereco_numero,
        cast(COMPLEMENT as string) as endereco_complemento,
        cast(BAIRRO as string) as endereco_bairro,
        cast(COD_CEP as string) as endereco_cep,
        cast(NU_LATITUDE as string) as endereco_latitude,
        cast(NU_LONGITUDE as string) as endereco_longitude,
        cast(TELEFONE as string) as telefone,
        cast(FAX as string) as fax,
        cast(E_MAIL as string) as email,
        cast(NO_URL as string) as url,
        cast(CPF as string) as cpf,
        cast(CNPJ as string) as cnpj,
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
        cast(NUM_ALVARA as string) as alvara_numero,
        safe_cast(DATA_EXPED as date) as alvara_data_expedicao,
        case
            when trim(IND_ORGEXP)='1' then 'SES'
            when trim(IND_ORGEXP)='2' then 'SMS'
            else null
        end as alvara_orgao_expedidor,
        safe_cast(DT_VAL_LIC_SANI as date) as licenca_sanitaria_data_validade,
        case
            when trim(TP_LIC_SANI)='1' then 'Total'
            when trim(TP_LIC_SANI)='2' then 'Parcial/Restrições'
            else null
        end as licenca_sanitaria_tipo,
        cast(CPFDIRETORCLINICO as string) as diretor_clinico_cpf,
        cast(REGDIRETORCLINICO as string) as diretor_clinico_conselho,
        case
            when trim(FL_ADESAO_FILANTROP) = '1' then true
            when trim(FL_ADESAO_FILANTROP) = '2' then false
            else null
        end as adesao_hospital_filantropico,
        safe_cast(DATA_ATU as date) as data_atualizacao_registro,
        cast(USUARIO as string) as usuario_atualizador_registro,
        safe_cast(DT_ATU_GEO as date) as data_atualizacao_geolocalizacao,
        cast(NO_USUARIO_GEO as string) as usuario_atualizador_geolocalizacao,
        case 
            when lower(trim(ST_GERACREDITO_GERENTE_SGIF))='s' then true
            when lower(trim(ST_GERACREDITO_GERENTE_SGIF))='n' then false
            else null
        end as gera_credito_gerente_sgif,

        -- Podem ser usados posteriormente para deduplicação
        data_particao,
        _loaded_at as data_carga,
    from source
)

select *
from renamed
