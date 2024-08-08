{{
    config(
        alias="paciente_historico",
        materialized="table",
    )
}}

select
    -- PK
    safe_cast(null as string) as id,

    -- Outras Chaves
    safe_cast(NULLIF(N_CPF, 'None') as string) as cpf,
    safe_cast(NULLIF(N_DNV, 'None') as string) as dnv,
    safe_cast(NULLIF(NIS, 'None') as string) as nis,
    safe_cast(NULLIF(N_CNS_DA_PESSOA_CADASTRADA, 'None') as string) as cns,

    -- Informações Pessoais
    safe_cast(NULLIF(NOME_DA_PESSOA_CADASTRADA, 'None') as string) as nome,
    safe_cast(NULLIF(NOME_SOCIAL_DA_PESSOA_CADASTRADA, 'None') as string) as nome_social,
    safe_cast(NULLIF(NOME_DA_MAE_PESSOA_CADASTRADA, 'None') as string) as nome_mae,
    safe_cast(null as string) as nome_pai,
    SAFE_CAST(CASE WHEN OBITO IS NULL OR OBITO = 'None' THEN 'False' ELSE 'True' END AS string) AS obito,
    safe_cast(NULLIF(OBITO, 'None') as date format 'DD/MM/YYYY') as data_obito,
    safe_cast(NULLIF(SEXO, 'None') as string) as sexo,
    safe_cast(NULLIF(ORIENTACAO_SEXUAL, 'None') as string) as orientacao_sexual,
    safe_cast(NULLIF(IDENTIDADE_GENERO, 'None') as string) as identidade_genero,
    safe_cast(NULLIF(RACA_COR, 'None') as string) as raca_cor,

    -- Contato
    safe_cast(NULLIF(EMAIL_CONTATO, 'None') as string) as email,
    safe_cast(NULLIF(TELEFONE_CONTATO, 'None') as string) as telefone,

    -- Nascimento
    safe_cast(NULLIF(NACIONALIDADE, 'None') as string) as nacionalidade,
    safe_cast(NULLIF(DATA_DE_NASCIMENTO, 'None') as date format 'DD/MM/YYYY') as data_nascimento,
    safe_cast(NULLIF(PAIS_DE_NASCIMENTO, 'None') as string) as pais_nascimento,
    safe_cast(NULLIF(MUNICIPIO_DE_NASCIMENTO, 'None') as string) as municipio_nascimento,
    safe_cast(null as string) as estado_nascimento,

    -- Informações da Unidade
    safe_cast(NULLIF(AP, 'None') as string) as ap,
    safe_cast(NULLIF(CODIGO_MICROAREA, 'None') as string) as microarea,
    safe_cast(NULLIF(NUMERO_CNES_UNIDADE, 'None') as string) as cnes_unidade,
    safe_cast(null as string) as nome_unidade,
    safe_cast(NULLIF(CODIGO_DA_EQUIPE_DE_SAUDE, 'None') as string) as codigo_equipe_saude,
    safe_cast(NULLIF(CODIGO_INE_EQUIPE_DE_SAUDE, 'None') as string) as codigo_ine_equipe_saude,
    safe_cast(null as timestamp) as data_atualizacao_vinculo_equipe,
    safe_cast(NULLIF(N_DO_PRONTUARIO, 'None') as string) as numero_prontuario,
    safe_cast(NULLIF(N_DA_FAMILIA, 'None') as string) as numero_familia,
    safe_cast(null as string) as cadastro_permanente,
    safe_cast(NULLIF(SITUACAO_USUARIO, 'None') as string) as situacao_usuario,
    safe_cast(NULLIF(DATA_CADASTRO, 'None') as timestamp format 'DD/MM/YYYY') as data_cadastro_inicial,
    safe_cast(NULLIF(DATA_ULTIMA_ATUALIZACAO_DO_CADASTRO, 'None') as timestamp format 'DD/MM/YYYY') as data_ultima_atualizacao_cadastral,

    -- Endereço
    safe_cast(NULLIF(TIPO_DE_DOMICILIO, 'None') as string) as endereco_tipo_domicilio,
    safe_cast(NULLIF(TIPO_DE_LOGRADOURO, 'None') as string) as endereco_tipo_logradouro,
    safe_cast(NULLIF(CEP_LOGRADOURO, 'None') as string) as endereco_cep,
    safe_cast(NULLIF(LOGRADOURO, 'None') as string) as endereco_logradouro,
    safe_cast(NULLIF(BAIRRO_DE_MORADIA, 'None') as string) as endereco_bairro,
    safe_cast(null as string) as endereco_estado,
    safe_cast(null as string) as endereco_municipio,

    -- Metadata columns
    safe_cast(null as date) as data_particao,
    safe_cast(NULLIF(updated_at, 'None') as timestamp) as updated_at,
    safe_cast(NULLIF(imported_at, 'None') as timestamp) as imported_at
from {{ source("brutos_prontuario_vitacare_staging", "paciente_historico_eventos") }}
