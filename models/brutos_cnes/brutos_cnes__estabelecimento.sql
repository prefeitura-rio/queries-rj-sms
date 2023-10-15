{{
    config(
        alias="estabelecimento",
    )
}}

with source as (select * from {{ source("brutos_cnes_staging", "tbEstabelecimento") }})

select
    -- Primary Key
    safe_cast(co_unidade as string) as id_unidade,

    -- Foreign Keys
    safe_cast(co_cnes as string) as id_cnes,
    safe_cast(tp_unidade as string) as id_tipo_unidade,
    safe_cast(co_tipo_estabelecimento as string) as id_tipo_estabelecimento,
    safe_cast(co_atividade_principal as string) as id_atividade_principal,
    safe_cast(co_estado_gestor as string) as id_estado_gestor,
    safe_cast(co_municipio_gestor as string) as id_municipio_gestor,
    safe_cast(co_regiao_saude as string) as id_regiao_saude,
    safe_cast(co_micro_regiao as string) as id_micro_regiao,
    safe_cast(co_distrito_sanitario as string) as id_distrito_sanitario,
    safe_cast(co_distrito_administrativo as string) as id_distrito_administrativo,
    safe_cast(co_atividade as string) as id_atividade_ensino_pesquisa,
    safe_cast(co_clientela as string) as id_clientela,
    safe_cast(co_turno_atendimento as string) as id_turno_atendimento,
    safe_cast(co_motivo_desab as string) as id_motivo_desativacao,
    safe_cast(co_natureza_jur as string) as id_natureza_juridica,

    -- Common fields
    safe_cast(nu_cnpj_mantenedora as string) as cnpj_mantenedora,
    safe_cast(tp_gestao as string) as tipo_gestao,
    safe_cast(tp_pfpj as string) as tipo_pfpj,
    safe_cast(nivel_dep as string) as dependencia_nivel,
    safe_cast(st_contrato_formalizado as string) as contrato_sus,
    safe_cast(no_razao_social as string) as nome_razao_social,
    safe_cast(no_fantasia as string) as nome_fantasia,
    safe_cast(no_logradouro as string) as endereco_logradouro,
    safe_cast(nu_endereco as string) as endereco_numero,
    safe_cast(no_complemento as string) as endereco_complemento,
    safe_cast(no_bairro as string) as endereco_bairro,
    safe_cast(co_cep as string) as endereco_cep,
    safe_cast(nu_latitude as float64) as endereco_latitude,
    safe_cast(nu_longitude as float64) as endereco_longitude,
    safe_cast(nu_telefone as string) as telefone,
    safe_cast(nu_fax as string) as fax,
    safe_cast(no_email as string) as email,
    safe_cast(no_url as string) as url,
    safe_cast(nu_cpf as string) as cpf,
    safe_cast(nu_cnpj as string) as cnpj,
    safe_cast(tp_estab_sempre_aberto as string) as aberto_sempre,
    safe_cast(st_conexao_internet as string) as conexao_internet,
    safe_cast(nu_alvara as string) as alvara_numero,
    safe_cast(dt_expedicao as string) as alvara_data_expedicao,  -- data
    safe_cast(tp_orgao_expedidor as string) as alvara_orgao_expedidor,
    safe_cast(dt_val_lic_sani as string) as licenca_sanitaria_data_validade,
    safe_cast(tp_lic_sani as string) as licenca_sanitaria_tipo,
    safe_cast(co_cpfdiretorcln as string) as diretor_clinico_cpf,
    safe_cast(reg_diretorcln as string) as diretor_clinico_conselho,
    safe_cast(st_adesao_filantrop as string) as adesao_hospital_filantropico,
    safe_cast(st_geracredito_gerente_sgif as string) as gera_credito_gerente_sgif,
    -- safe_cast(CO_TIPO_UNIDADE as string),  -- campo sem uso (fonte: CNES)
    -- safe_cast(NO_FANTASIA_ABREV as string),  -- campo sem uso (fonte: CNES)
    -- Metadata
    safe_cast(
        dt_atualizacao_origem as date format 'DD/MM/YYYY'
    ) as data_entrada_sistema,
    safe_cast(dt_atualizacao as date format 'DD/MM/YYYY') as data_atualizao_registro,
    safe_cast(co_usuario as string) as usuario_atualizador_registro,
    safe_cast(
        dt_atu_geo as date format 'DD/MM/YYYY'
    ) as data_atualizacao_geolocalizacao,
    safe_cast(no_usuario_geo as string) as usuario_atualizador_geolocalizacao,
    safe_cast(_data_carga as datetime) as data_carga,
    safe_cast(_data_snapshot as datetime) as data_snapshot
from source
