{{
    config(
        enabled= false,
        alias="paciente_suspeitos",
        materialized="table",
        schema="saude_historico_clinico"
    )
}}

-- This code integrates patient data from three sources:
-- rj-sms.brutos_prontuario_vitacare.paciente (VITACARE)
-- rj-sms.brutos_plataforma_vitai.paciente (VITAI)
-- rj-sms.brutos_plataforma_smsrio.paciente (SMSRIO)
-- The goal is to consolidate information such as registration data,
-- contact, address and medical record into a single view.
-- dbt run --select int_historico_clinico__paciente__vitacare int_historico_clinico__paciente__smsrio int_historico_clinico__paciente__vitai mart_historico_clinico__paciente

-- Declaration of the variable to filter by CPF (optional)
-- DECLARE cpf_filter STRING DEFAULT "";

-- VITACARE: Patient base table
WITH vitacare_tb AS (
    SELECT 
        cpf,
        cns,
        STRUCT(
            dados.nome,
            dados.cpf_valido_indicador,
            dados.nome_social,
            dados.data_nascimento,
            dados.genero,
            dados.raca,
            dados.obito_indicador,
            dados.obito_data,
            dados.mae_nome,
            dados.pai_nome,
            dados.metadados,
            dados.rank
        ) AS dados,
        contato,
        endereco,
        prontuario
    FROM {{ ref('int_historico_clinico__paciente__vitacare') }},
    UNNEST(dados) AS dados
    -- AND cpf = cpf_filter
),

-- VITAI: Patient base table
vitai_tb AS (
    SELECT 
        cpf,
        cns,
        STRUCT(
            dados.nome,
            dados.cpf_valido_indicador,
            dados.nome_social,
            dados.data_nascimento,
            dados.genero,
            dados.raca,
            dados.obito_indicador,
            dados.obito_data,
            dados.mae_nome,
            dados.pai_nome,
            dados.metadados,
            dados.rank
        ) AS dados,
        contato,
        endereco,
        prontuario
    FROM {{ ref('int_historico_clinico__paciente__vitai') }},
    UNNEST(dados) AS dados
    -- AND cpf = cpf_filter
),

-- SMSRIO: Patient base table
smsrio_tb AS (
    SELECT 
        cpf,
        cns,
        STRUCT(
            dados.nome,
            dados.cpf_valido_indicador,
            dados.nome_social,
            dados.data_nascimento,
            dados.genero,
            dados.raca,
            dados.obito_indicador,
            dados.obito_data,
            dados.mae_nome,
            dados.pai_nome,
            dados.metadados,
            dados.rank
        ) AS dados,
        contato,
        endereco,
        prontuario
    FROM {{ ref("int_historico_clinico__paciente__smsrio") }},
    UNNEST(dados) AS dados
    -- AND cpf = cpf_filter
),

---=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--
--  Merge data from different sources
---=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--

-- Paciente Dados: Merges patient data
all_data AS (
    SELECT 
        *
    FROM (
        SELECT 
            *
        FROM vitacare_tb
        UNION ALL
        SELECT 
            *
        FROM vitai_tb
        UNION ALL
        SELECT 
            *
        FROM smsrio_tb
    )
)

SELECT 
    * 
FROM all_data