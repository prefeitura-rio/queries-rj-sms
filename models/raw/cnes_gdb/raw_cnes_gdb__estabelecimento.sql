{{
    config(
        alias="estabelecimento",
        schema= "brutos_cnes_gdb"
    )
}}

with source as (
      select * from {{ source('brutos_cnes_gdb_staging', 'estabelecimento') }}
),
renamed as (
select 
    cast(UNIDADE_ID as string) as id_unidade,
    cast(CNES as string) as id_cnes,
    cast(TP_UNID_ID as string) as id_tipo_unidade,
    cast(CO_TIPO_ESTABELECIMENTO as string) as id_tipo_estabelecimento,
    cast(CO_ATIVIDADE_PRINCIPAL as string) as id_atividade_principal,
    cast(CODMUNGEST as string) as id_municipio_gestor,
    cast(SIGESTGEST as string) as estado_gestor_sigla,
    cast(REG_SAUDE as string) as id_regiao_saude,
    cast(MICRO_REG as string) as id_micro_regiao,
    cast(DIST_SANIT as string) as id_distrito_sanitario,
    cast(DIST_ADMIN as string) as id_distrito_administrativo,
    cast(COD_ATIV as string) as id_atividade,
    cast(COD_CLIENT as string) as id_cliente,
    cast(COD_TURNAT as string) as id_turno_atendimento,
    cast(CD_MOTIVO_DESAB as string) as id_motivo_desabilitacao, 
    cast(CO_NATUREZA_JUR as string) as id_natureza_juridica,
    cast(CNPJ_MANT as string) as cnpj_mantenedora,
    case 
        when PFPJ_IND='1' then 'Pessoa física'
        when PFPJ_IND='3' then 'Pessoa jurídica'
        else null
    end as tipo_pessoa,
    case 
        when NIVEL_DEP='1' then 'Individual'
        when NIVEL_DEP='3' then 'Mantido'
        else null
    end as dependencia_nivel,
    case 
        when ST_CONTRATO_FORMALIZADO='S' then true
        when ST_CONTRATO_FORMALIZADO='N' then false
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
        when TP_ESTAB_SEMPRE_ABERTO='S' then true
        when TP_ESTAB_SEMPRE_ABERTO='N' then false
        else null
    end as aberto_sempre,    
    case
        when ST_CONEXAOINTERNET='S' then true
        when ST_CONEXAOINTERNET='N' then false
        else null
    end as possui_conexao_internet,
    cast(NUM_ALVARA as string) as alvara_numero,
    cast(DATA_EXPED as date) as alvara_data_expedicao,
    case 
        when IND_ORGEXP='1' then 'SES'
        when IND_ORGEXP='2' then 'SMS'
        else null
    end as alvara_orgao_expedidor,
    cast(DT_VAL_LIC_SANI as date) as licenca_sanitaria_data_validade,
    case 
        when TP_LIC_SANI='1' then 'Total'
        when TP_LIC_SANI='2' then 'Parcial/Restrições'
        else null
    end as licenca_sanitaria_tipo,
    cast(CPFDIRETORCLINICO as string) as diretor_clinico_cpf,
    cast(REGDIRETORCLINICO as string) as diretor_clinico_conselho,
    case 
        when FL_ADESAO_FILANTROP = '1' then true
        when FL_ADESAO_FILANTROP = '2' then false
        else null
    end as adesao_hospital_filantropico,
    cast(DATA_ATU as date) as data_atualizacao_registro,
    cast(USUARIO as string) as usuario_atualizador_registro,
    cast(DT_ATU_GEO as date) as data_atualizacao_geolocalizacao,
    cast(NO_USUARIO_GEO as string) as usuario_atualizador_geolocalizacao,
    case 
        when ST_GERACREDITO_GERENTE_SGIF='S' then true
        when ST_GERACREDITO_GERENTE_SGIF='N' then false
        else null
    end as gera_credito_gerente_sgif,

from source
)
select * from renamed   