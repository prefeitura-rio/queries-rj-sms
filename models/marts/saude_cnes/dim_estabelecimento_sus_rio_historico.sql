{{
    config(
        schema="mart_saude_cnes__estabelecimento_sus_rio_historico",
        alias="estabelecimento_sus_rio_historico",
    )
}}

WITH

-- Obtendo a data mais atual
versao_atual AS (
    SELECT MAX(data_particao) AS versao 
    FROM {{ ref("raw_cnes_web__tipo_unidade") }}
),

-- Obtendo todos os estabelecimentos do MRJ que possuem vinculo com o SUS
estabelecimentos_brutos AS (
    SELECT
        ano,
        mes,
        cep,
        id_estabelecimento_cnes,
        id_natureza_juridica,
        tipo_gestao,
        tipo_unidade,
        tipo_turno,
        indicador_vinculo_sus,
        indicador_atendimento_internacao_sus,	
        indicador_atendimento_ambulatorial_sus,
        indicador_atendimento_sadt_sus,
        indicador_atendimento_urgencia_sus,  
        indicador_atendimento_outros_sus, 
        indicador_atendimento_vigilancia_sus,
        indicador_atendimento_regulacao_sus
    FROM {{ source("brutos_cnes_ftp", "estabelecimento") }}
    WHERE 
        sigla_uf = "RJ"
        AND id_municipio_6 = "330455"
        AND (
            indicador_vinculo_sus = 1
            OR indicador_atendimento_internacao_sus = 1 	
            OR indicador_atendimento_ambulatorial_sus = 1
            OR indicador_atendimento_sadt_sus = 1
            OR indicador_atendimento_urgencia_sus = 1 
            OR indicador_atendimento_outros_sus = 1
            OR indicador_atendimento_vigilancia_sus = 1
            OR indicador_atendimento_regulacao_sus = 1
        )
),

-- Obtendo atributos dos estabelecimentos via tabela desnormalizada proveniente do CNES WEB
estabelecimentos_atributos_cnes_web AS (
    SELECT
        id_cnes,
        nome_razao_social,
        nome_fantasia,
        cnpj_mantenedora,
        endereco_bairro,
        endereco_logradouro,
        endereco_numero,
        endereco_complemento,
        endereco_latitude,
        endereco_longitude,
        id_motivo_desativacao,
        id_unidade,
        aberto_sempre,
        diretor_clinico_cpf,
        diretor_clinico_conselho,
        data_atualizao_registro,
        usuario_atualizador_registro,
        mes_particao,
        ano_particao,
        data_particao,
        data_carga,
        data_snapshot,
        id_distrito_sanitario,
        telefone,
        email
    FROM {{ ref("raw_cnes_web__estabelecimento") }}
    WHERE data_particao = (SELECT versao FROM versao_atual)
),

-- Obtendo atributos dos estabelecimentos via tabela proveniente da SUBGERAL
estabelecimentos_atributos_subgeral AS (
    SELECT 
        ESFERA AS esfera,
        AP AS id_ap,
        AP_TITULO AS ap,
        CNES AS id_estabelecimento_cnes,
        TIPO_UNIDADE_AGRUPADO AS tipo_unidade_agrupado
    FROM {{ source("subgeral_padronizacoes", "padronizacao_estabelecimentos") }}
),

-- Obtendo atributos dos estabelecimentos via tabela proveniente da DIT
estabelecimentos_atributos_dit AS (
    SELECT * 
    FROM {{ ref("raw_sheets__estabelecimento_auxiliar") }}
),

-- Obtendo atributos de contato para os estabelecimentos
contatos_aps AS (
    SELECT * 
    FROM {{ ref("raw_plataforma_smsrio__estabelecimento_contato") }}
),

-- Carregando tabelas utilizadas para mapear códigos em suas descrições textuais
tp_gestao AS (
    SELECT 
        DISTINCT id_tipo_gestao AS id_tipo_gestao,
        UPPER(tipo_gestao_descr) AS tipo_gestao_descr
    FROM {{ source("brutos_datasus", "tipo_gestao") }}
),

nat_jur AS (
    SELECT 
        id_natureza_juridica, 
        descricao AS natureza_juridica_descr 
    FROM {{ ref("raw_cnes_web__natureza_juridica") }}
    WHERE data_particao = (SELECT versao FROM versao_atual)

),

tp_unidade AS (
    SELECT 
        id_tipo_unidade,
        descricao AS tipo_unidade_descr 
    FROM {{ ref("raw_cnes_web__tipo_unidade") }}
    WHERE data_particao = (SELECT versao FROM versao_atual)
),

turno AS (
    SELECT 
        id_turno_atendimento, 
        descricao AS turno_atendimento 
    FROM {{ ref("raw_cnes_web__turno_atendimento") }}
    WHERE data_particao = (SELECT versao FROM versao_atual)
),

-- Juntando todos os atributos e mappings, para enriquecer a tabela final de estabelecimentos
estabelecimentos_final AS (
    SELECT
        brutos.*,
        cnes_web.*,
        subgeral.tipo_unidade_agrupado,
        subgeral.esfera,
        subgeral.id_ap,
        subgeral.ap,
        tp_gestao.tipo_gestao_descr,
        nat_jur.natureza_juridica_descr,
        tp_unidade.tipo_unidade_descr,
        turno.turno_atendimento,
        SPLIT(cnes_web.telefone, "/") AS telefone_cnes,
        contatos_aps.telefone AS telefone_aps,
        cnes_web.email AS email_cnes,
        contatos_aps.email AS email_aps,
        contatos_aps.facebook,
        contatos_aps.instagram,
        contatos_aps.twitter,
        estabelecimentos_atributos_dit.agrupador_sms,
        estabelecimentos_atributos_dit.tipo_sms,
        estabelecimentos_atributos_dit.tipo_sms_simplificado,
        estabelecimentos_atributos_dit.nome_limpo,
        REGEXP_REPLACE(
            estabelecimentos_atributos_dit.nome_limpo,
            r'(CF |CSE |CMS |UPA 24h |POLICLINICA |HOSPITAL MUNICIPAL |COORD DE EMERGENCIA REGIONAL CER |MATERNIDADE )',
            ''
        ) AS nome_complemento,
        estabelecimentos_atributos_dit.nome_sigla,
        estabelecimentos_atributos_dit.prontuario_tem,
        estabelecimentos_atributos_dit.prontuario_versao,
        estabelecimentos_atributos_dit.responsavel_sms,
        estabelecimentos_atributos_dit.administracao,
        estabelecimentos_atributos_dit.prontuario_estoque_tem_dado,
        estabelecimentos_atributos_dit.prontuario_estoque_motivo_sem_dado,
        COALESCE(
            estabelecimentos_atributos_dit.area_programatica, cnes_web.id_distrito_sanitario
        ) AS id_distrito_sanitario_corrigido,  
        CASE 
            WHEN (cnes_web.nome_fantasia LIKE 'SMS %' OR cnes_web.nome_fantasia LIKE 'SMSDC %' OR cnes_web.nome_fantasia = 'RIOSAUDE') THEN 1
            ELSE 0 
        END AS indicador_estabelecimento_sms
    FROM estabelecimentos_brutos AS brutos
    LEFT JOIN estabelecimentos_atributos_cnes_web AS cnes_web ON brutos.id_estabelecimento_cnes = cnes_web.id_cnes
    LEFT JOIN estabelecimentos_atributos_subgeral AS subgeral ON brutos.id_estabelecimento_cnes = subgeral.id_estabelecimento_cnes
    LEFT JOIN nat_jur ON brutos.id_natureza_juridica = nat_jur.id_natureza_juridica
    LEFT JOIN tp_unidade ON brutos.tipo_unidade = tp_unidade.id_tipo_unidade
    LEFT JOIN turno ON brutos.tipo_turno = turno.id_turno_atendimento
    LEFT JOIN tp_gestao ON brutos.tipo_gestao = tp_gestao.id_tipo_gestao
    LEFT JOIN estabelecimentos_atributos_dit ON brutos.id_estabelecimento_cnes = estabelecimentos_atributos_dit.id_cnes
    LEFT JOIN contatos_aps ON brutos.id_estabelecimento_cnes = contatos_aps.id_cnes
)

-- Seleção final
SELECT 
    ano,
    mes,
    esfera,
    id_distrito_sanitario_corrigido AS area_programatica,
    id_ap,
    ap,
    cep AS endereco_cep,
    id_estabelecimento_cnes AS id_cnes,
    nome_razao_social,
    nome_fantasia,
    id_natureza_juridica,
    natureza_juridica_descr,
    tipo_gestao,
    tipo_gestao_descr,
    tipo_unidade AS id_tipo_unidade,
    tipo_unidade_descr AS tipo,  -- Renomear para tipo_cnes
    tipo_unidade_agrupado,
    tipo_turno,
    turno_atendimento,
    agrupador_sms AS tipo_sms_agrupado,
    tipo_sms,
    tipo_sms_simplificado,
    cnpj_mantenedora,
    endereco_bairro,
    endereco_logradouro,
    endereco_numero,
    endereco_complemento,
    endereco_latitude,
    endereco_longitude,
    IF(id_motivo_desativacao = "", "sim", "não") AS ativa,
    indicador_estabelecimento_sms,
    indicador_vinculo_sus,
    indicador_atendimento_internacao_sus,	
    indicador_atendimento_ambulatorial_sus,
    indicador_atendimento_sadt_sus,
    indicador_atendimento_urgencia_sus,  
    indicador_atendimento_outros_sus, 
    indicador_atendimento_vigilancia_sus,
    indicador_atendimento_regulacao_sus,
    id_unidade,
    nome_limpo,
    nome_sigla,
    nome_complemento,
    responsavel_sms,
    administracao,
    prontuario_tem,
    prontuario_versao,
    prontuario_estoque_tem_dado,
    prontuario_estoque_motivo_sem_dado,
    COALESCE(telefone_aps, telefone_cnes) AS telefone,
    COALESCE(email_aps, email_cnes) AS email,
    facebook,
    instagram,
    twitter,
    aberto_sempre,
    diretor_clinico_cpf,
    diretor_clinico_conselho,

    -- Metadata
    data_atualizao_registro,
    usuario_atualizador_registro,
    mes_particao,
    ano_particao,
    data_particao,
    data_carga,
    data_snapshot
FROM estabelecimentos_final
