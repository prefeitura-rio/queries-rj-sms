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
), -- OBS: Não faz mais sentido pegar a versão de alguma outra tabela, sem ser tipo_unidade?

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

-- Obtendo atributos auxiliares dos estabelecimentos via classificação interna construída pela SMS
estabelecimentos_atributos AS (
    SELECT
        id_cnes,
        indicador_estabelecimento_sms,
        tipo_unidade_subgeral,
        tipo_unidade_agrupado_subgeral,
        esfera_subgeral,
        area_programatica,
        area_programatica_descr,
        agrupador_sms,
        tipo_sms,
        tipo_sms_simplificado,
        nome_limpo,
        nome_sigla,
        prontuario_tem,
        prontuario_versao,
        responsavel_sms,
        administracao,
        prontuario_estoque_tem_dado,
        prontuario_estoque_motivo_sem_dado
    FROM {{ ref("raw_sheets__estabelecimento_auxiliar") }}
),

-- Obtendo atributos de contato para os estabelecimentos
contatos_aps AS (
    SELECT
        id_cnes,
        telefone,
        email,
        facebook,
        instagram,
        twitter
    FROM {{ ref("raw_plataforma_smsrio__estabelecimento_contato") }}
),

-- Carregando tabelas utilizadas para mapear códigos em suas descrições textuais
tp_gestao AS (
    SELECT * FROM UNNEST([
        STRUCT("D" AS id_tipo_gestao, "DUPLA" AS tipo_gestao_descr),
        STRUCT("E", "ESTADUAL"),
        STRUCT("M", "MUNICIPAL"),
        STRUCT("Z", "SEM GESTAO"),
        STRUCT("S", "SEM GESTAO"),
        STRUCT("-Z", "NAO INFORMADO")
    ])
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
        -- Brutos FTP DATASUS
        brutos.ano,
        brutos.mes,
        brutos.cep,
        brutos.id_estabelecimento_cnes,
        brutos.id_natureza_juridica,
        brutos.tipo_gestao,
        brutos.tipo_unidade,
        brutos.tipo_turno,
        brutos.indicador_vinculo_sus,
        brutos.indicador_atendimento_internacao_sus,
        brutos.indicador_atendimento_ambulatorial_sus,
        brutos.indicador_atendimento_sadt_sus,
        brutos.indicador_atendimento_urgencia_sus,
        brutos.indicador_atendimento_outros_sus,
        brutos.indicador_atendimento_vigilancia_sus,
        brutos.indicador_atendimento_regulacao_sus,

        -- CNES Web
        cnes_web.nome_razao_social,
        cnes_web.nome_fantasia,
        cnes_web.cnpj_mantenedora,
        cnes_web.endereco_bairro,
        cnes_web.endereco_logradouro,
        cnes_web.endereco_numero,
        cnes_web.endereco_complemento,
        cnes_web.endereco_latitude,
        cnes_web.endereco_longitude,
        cnes_web.id_motivo_desativacao,
        cnes_web.id_unidade,
        cnes_web.aberto_sempre,
        cnes_web.diretor_clinico_cpf,
        cnes_web.diretor_clinico_conselho,
        cnes_web.data_atualizao_registro,
        cnes_web.usuario_atualizador_registro,
        cnes_web.mes_particao,
        cnes_web.ano_particao,
        cnes_web.data_particao,
        cnes_web.data_carga,
        cnes_web.data_snapshot,
        cnes_web.email AS email_cnes,
        SPLIT(cnes_web.telefone, "/") AS telefone_cnes,

        -- Atributos criados in house
        estabelecimentos_atributos.indicador_estabelecimento_sms,
        estabelecimentos_atributos.tipo_unidade_subgeral AS tipo_unidade_alternativo,
        estabelecimentos_atributos.tipo_unidade_agrupado_subgeral AS tipo_unidade_agrupado,
        estabelecimentos_atributos.esfera_subgeral AS esfera,
        estabelecimentos_atributos.area_programatica AS id_ap,
        estabelecimentos_atributos.area_programatica_descr AS ap,
        estabelecimentos_atributos.agrupador_sms,
        estabelecimentos_atributos.tipo_sms,
        estabelecimentos_atributos.tipo_sms_simplificado,
        estabelecimentos_atributos.nome_limpo,
        estabelecimentos_atributos.nome_sigla,
        estabelecimentos_atributos.prontuario_tem,
        estabelecimentos_atributos.prontuario_versao,
        estabelecimentos_atributos.responsavel_sms,
        estabelecimentos_atributos.administracao,
        estabelecimentos_atributos.prontuario_estoque_tem_dado,
        estabelecimentos_atributos.prontuario_estoque_motivo_sem_dado,
            REGEXP_REPLACE(
            estabelecimentos_atributos.nome_limpo,
            r'(CF |CSE |CMS |UPA 24h |POLICLINICA |HOSPITAL MUNICIPAL |COORD DE EMERGENCIA REGIONAL CER |MATERNIDADE )',
            ''
        ) AS nome_complemento,

        -- Mappings oficiais do CNES / DATASUS
        tp_gestao.tipo_gestao_descr,
        nat_jur.natureza_juridica_descr,
        tp_unidade.tipo_unidade_descr,
        turno.turno_atendimento,

        -- Informações de Contato
        contatos_aps.telefone AS telefone_aps,
        contatos_aps.facebook,
        contatos_aps.instagram,
        contatos_aps.twitter,
        contatos_aps.email AS email_aps,

    FROM estabelecimentos_brutos AS brutos
    LEFT JOIN estabelecimentos_atributos_cnes_web AS cnes_web ON brutos.id_estabelecimento_cnes = cnes_web.id_cnes
    LEFT JOIN nat_jur ON brutos.id_natureza_juridica = nat_jur.id_natureza_juridica
    LEFT JOIN tp_unidade ON brutos.tipo_unidade = tp_unidade.id_tipo_unidade
    LEFT JOIN turno ON brutos.tipo_turno = turno.id_turno_atendimento
    LEFT JOIN tp_gestao ON brutos.tipo_gestao = tp_gestao.id_tipo_gestao
    LEFT JOIN estabelecimentos_atributos ON brutos.id_estabelecimento_cnes = estabelecimentos_atributos.id_cnes
    LEFT JOIN contatos_aps ON brutos.id_estabelecimento_cnes = contatos_aps.id_cnes
)

-- Seleção final
SELECT 
    -- Identificação
    ano,
    mes,
    id_estabelecimento_cnes AS id_cnes,
    id_unidade,
    nome_razao_social,
    nome_fantasia,
    nome_limpo,
    nome_sigla,
    nome_complemento,
    cnpj_mantenedora,

    -- Responsabilização
    esfera,
    id_natureza_juridica,
    natureza_juridica_descr,
    tipo_gestao,
    tipo_gestao_descr,
    responsavel_sms,
    administracao,
    diretor_clinico_cpf,
    diretor_clinico_conselho,

    -- Atributos
    tipo_turno,
    turno_atendimento,
    aberto_sempre,

    -- Tipagem dos Estabelecimentos (CNES)
    tipo_unidade AS id_tipo_unidade,
    tipo_unidade_descr AS tipo, -- Renomear para tipo_cnes?

    -- Tipagem dos Estabelecimentos (DIT)
    tipo_sms,
    tipo_sms_simplificado,
    agrupador_sms AS tipo_sms_agrupado,

    -- Tipagem dos Estabelecimentos (SUBGERAL)
    tipo_unidade_alternativo,
    tipo_unidade_agrupado,

    -- Localização
    id_ap,
    ap,
    cep AS endereco_cep,
    endereco_bairro,
    endereco_logradouro,
    endereco_numero,
    endereco_complemento,
    endereco_latitude,
    endereco_longitude,

    -- Status
    CASE 
        WHEN id_motivo_desativacao IS NULL OR id_motivo_desativacao = '' THEN 'sim' 
        ELSE 'não' 
    END AS ativa,

    -- Prontuário
    prontuario_tem,
    prontuario_versao,
    prontuario_estoque_tem_dado,
    prontuario_estoque_motivo_sem_dado,

    -- Informações de contato
    COALESCE(telefone_aps, telefone_cnes) AS telefone,
    COALESCE(email_aps, email_cnes) AS email,
    facebook,
    instagram,
    twitter,

    -- Indicadores
    indicador_estabelecimento_sms,
    indicador_vinculo_sus,
    indicador_atendimento_internacao_sus,
    indicador_atendimento_ambulatorial_sus,
    indicador_atendimento_sadt_sus,
    indicador_atendimento_urgencia_sus,
    indicador_atendimento_outros_sus,
    indicador_atendimento_vigilancia_sus,
    indicador_atendimento_regulacao_sus,

    -- Metadados
    data_atualizao_registro,
    usuario_atualizador_registro,
    mes_particao,
    ano_particao,
    data_particao,
    data_carga,
    data_snapshot

FROM estabelecimentos_final