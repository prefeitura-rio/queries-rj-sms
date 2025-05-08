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
    cast(CNPJ_MANT as string) as cnpj_mantenedora,
    case 
        when PFPJ_IND='1' then 'Pessoa Física'
        when PFPJ_IND='3' then 'Pessoa Jurídica'
        else null
    end as tipo_pessoa,
    case 
        when NIVEL_DEP='1' then 'Individual'
        when NIVEL_DEP='3' then 'Mantido'
        else null
    end as estabelecimento_situacao,
    cast(R_SOCIAL as string) as razao_social,
    cast(NOME_FANTA as string) as nome_fantasia,
    cast(LOGRADOURO as string) as logradouro,
    cast(NUMERO as string) as numero,
    cast(COMPLEMENT as string) as complemento,
    cast(BAIRRO as string) as bairro,
    cast(COD_CEP as string) as cep,
    cast(REG_SAUDE as string) as id_regiao_saude,
    cast(MICRO_REG as string) as id_micro_regiao,
    cast(DIST_SANIT as string) as id_distrito_sanitario,
    cast(DIST_ADMIN as string) as id_modulo_asssistencial,
    cast(TELEFONE as string) as telefone,
    cast(FAX as string) as fax,
    cast(E_MAIL as string) as email,
    cast(CPF as string) as cpf,
    cast(CNPJ as string) as cnpj,
    cast(COD_ATIV as string) as id_atividade,
    cast(COD_CLIENT as string) as id_cliente,
    cast(NUM_ALVARA as string) as alvara_numero,
    cast(DATA_EXPED as date) as alvara_data_expedicao,
    cast(DT_VAL_LIC_SANI as date) as alvara_data_validade,
    case 
        when TP_LIC_SANI='1' then 'Total'
        when TP_LIC_SANI='2' then 'Parcial/Restrições'
        else null
    end as alvara_tipo_licenca,
    case 
        when IND_ORGEXP='1' then 'SES'
        when IND_ORGEXP='2' then 'SMS'
        else null
    end as orgao_expedidor,
    cast(TP_UNID_ID as string) as id_tipo_unidade,
    cast(COD_TURNAT as string) as id_turno_atendimento,
    cast(SIGESTGEST as string) as estado_gestor_sigla,
    cast(CODMUNGEST as string) as id_municipio_gestor,
    case 
        when STATUSMOV='1' then 'Não aprovado'
        when STATUSMOV='2' then 'Consistido'
        when STATUSMOV='3' then 'Exportado'
        when STATUSMOV='B' then 'Registro Bloqueado'
        when STATUSMOV='U' then 'Registro em Uso'
        else null
    end as status_unidade,
    -- TP_PRESTADOR,
    cast(DATA_ATU as date) as data_ultima_atualizacao,
    cast(USUARIO as string) as usuario_atualizador,
    cast(CPFDIRETORCLINICO as string) as cpf_diretor_clinico,
    cast(REGDIRETORCLINICO as string) as registro_diretor_clinico,
    cast(CD_MOTIVO_DESAB as string) as id_motivo_desabilitacao,
    case 
        when FL_ADESAO_FILANTROP = '1' then 'Sim'
        when FL_ADESAO_FILANTROP = '2' then 'Não'
        else null
    end as adesao_filantropia,
    -- CMPT_VIGENTE,
    cast(NO_URL as string) as url,
    cast(NU_LATITUDE as string) as latitude,
    cast(NU_LONGITUDE as string) as longitude,
    cast(DT_ATU_GEO as date) as data_atualizacao_geolocalizacao,
    cast(NO_USUARIO_GEO as string) as usuario_atualizador_geolocalizacao,
    cast(CO_NATUREZA_JUR as string) as id_natureza_juridica,
    case 
        when TP_ESTAB_SEMPRE_ABERTO='S' then 'Sim'
        when TP_ESTAB_SEMPRE_ABERTO='N' then 'Não'
        else null
    end as sempre_aberto,
    case 
        when ST_GERACREDITO_GERENTE_SGIF='S' then 'Sim'
        when ST_GERACREDITO_GERENTE_SGIF='N' then 'Não'
        else null
    end as gera_credito_gerente_sgif,
    cast(ST_NAT_JUR_WEBSERVICE as string) as id_natureza_juridica_webservice,
    cast(ST_DADOS_CADONLINE_WEBSERV as string) as dados_cadonline_webservice,
    case
        when ST_CONEXAOINTERNET='S' then 'Sim'
        when ST_CONEXAOINTERNET='N' then 'Não'
        else null
    end as possui_conexao_internet,
    cast(CO_TIPO_ESTABELECIMENTO as string) as id_tipo_estabelecimento,
    cast(CO_ATIVIDADE_PRINCIPAL as string) as id_atividade_principal,
    case 
        when ST_CONTRATO_FORMALIZADO='S' then 'Sim'
        when ST_CONTRATO_FORMALIZADO='N' then 'Não'
        else null
    end as contrato_formalizado_sus
from source
)
select * from renamed   