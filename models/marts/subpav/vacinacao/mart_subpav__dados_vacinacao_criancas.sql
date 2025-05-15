
{{
    config(
        enabled=true,
        alias="dados_vacinacao_criancas",
    )
}}

WITH 

    cns_criancas as (
        select valor_cns
        from {{ ref('mart_historico_clinico__paciente') }}, unnest(cns) as valor_cns
        where dados.data_nascimento > '2023-05-15'
    ),

    vacinas_detectadas AS (
        SELECT
            paciente_cns AS cns,
            FORMAT_DATE('%Y-%m-%d',paciente_nascimento_data) AS dt_nascimento,
            date_diff(CURRENT_DATE(), paciente_nascimento_data, month) as idade_meses,
            DATE_DIFF(DATE_TRUNC(aplicacao_data, MONTH),  DATE_TRUNC(paciente_nascimento_data, MONTH), MONTH) - CASE WHEN EXTRACT(DAY FROM aplicacao_data) < EXTRACT(DAY FROM paciente_nascimento_data) THEN 1 ELSE 0 END AS aplicacao_meses,
            DATE_DIFF(
                aplicacao_data,
                DATE_ADD(
                paciente_nascimento_data,
                INTERVAL (
                    DATE_DIFF(DATE_TRUNC(aplicacao_data, MONTH), DATE_TRUNC(paciente_nascimento_data, MONTH), MONTH) 
                    - CASE WHEN EXTRACT(DAY FROM aplicacao_data) < EXTRACT(DAY FROM paciente_nascimento_data) THEN 1 ELSE 0 END
                ) MONTH
                ),
                DAY
            ) AS aplicacao_dias,
            FORMAT_DATE('%Y-%m-%d', aplicacao_data) AS aplicacao_data,
            FORMAT_DATE('%Y%m%d', aplicacao_data) AS ordem,
            FORMAT_DATE('%d/%m/%Y', aplicacao_data) AS aplicacao_data_f,	
            dose,
            TRIM(REPLACE(REGEXP_REPLACE(dose, r'\s+', ' '), ' ', '_')) AS dose_f,
            ARRAY(
            SELECT DISTINCT vacina FROM UNNEST([
                IF(REGEXP_CONTAINS(REGEXP_REPLACE(NORMALIZE(descricao, NFD), r'\p{Mn}', ''), r'vacina tuberculose'), 'bcg', NULL),
                IF(REGEXP_CONTAINS(REGEXP_REPLACE(NORMALIZE(descricao, NFD), r'\p{Mn}', ''), r'vacina hepatite b'), 'hepatite_b', NULL),
                IF(REGEXP_CONTAINS(REGEXP_REPLACE(NORMALIZE(descricao, NFD), r'\p{Mn}', ''), r'vacina pentavalente'), 'pentavalente', NULL),
                IF(REGEXP_CONTAINS(REGEXP_REPLACE(NORMALIZE(descricao, NFD), r'\p{Mn}', ''), r'vacina poliomielite inativada'), 'poliomielite_inativada', NULL),
                IF(REGEXP_CONTAINS(REGEXP_REPLACE(NORMALIZE(descricao, NFD), r'\p{Mn}', ''), r'vacina pneumococica 10 - valente'), 'pneumococica_10_valente', NULL),
                IF(REGEXP_CONTAINS(REGEXP_REPLACE(NORMALIZE(descricao, NFD), r'\p{Mn}', ''), r'vacina oral rotavirus humano g1p1\[8\]  atenuada'), 'rotavirus', NULL),
                IF(REGEXP_CONTAINS(REGEXP_REPLACE(NORMALIZE(descricao, NFD), r'\p{Mn}', ''), r'vacina meningococica c  conjugada'), 'meningococica', NULL),
                IF(REGEXP_CONTAINS(REGEXP_REPLACE(NORMALIZE(descricao, NFD), r'\p{Mn}', ''), r'vacina contra a febre amarela'), 'febre_amarela', NULL),
                IF(REGEXP_CONTAINS(REGEXP_REPLACE(NORMALIZE(descricao, NFD), r'\p{Mn}', ''), r'vacina triplice viral  sarampo  caxumba  rubeola   atenuada'), 'triplice_viral', NULL)
            ]) AS vacina
            ) AS vacinas
        FROM
            {{ ref('raw_prontuario_vitacare__vacina') }}
        WHERE
            paciente_cns IN (select valor_cns from cns_criancas)
            AND FORMAT_DATE('%Y-%m', aplicacao_data) <= '2025-01'
            AND (
                REGEXP_REPLACE(NORMALIZE(descricao, NFD), r'\p{Mn}', '') LIKE '%vacina tuberculose%'
                OR REGEXP_REPLACE(NORMALIZE(descricao, NFD), r'\p{Mn}', '') LIKE '%vacina hepatite b%'                        
                OR REGEXP_REPLACE(NORMALIZE(descricao, NFD), r'\p{Mn}', '') LIKE '%vacina pentavalente%'
                OR REGEXP_REPLACE(NORMALIZE(descricao, NFD), r'\p{Mn}', '') LIKE '%vacina poliomielite inativada%'
                OR REGEXP_REPLACE(NORMALIZE(descricao, NFD), r'\p{Mn}', '') LIKE '%vacina pneumococica 10 - valente%'
                OR REGEXP_REPLACE(NORMALIZE(descricao, NFD), r'\p{Mn}', '') LIKE '%vacina oral rotavirus humano g1p1[8]  atenuada%'
                OR REGEXP_REPLACE(NORMALIZE(descricao, NFD), r'\p{Mn}', '') LIKE '%vacina meningococica c  conjugada%'
                OR REGEXP_REPLACE(NORMALIZE(descricao, NFD), r'\p{Mn}', '') LIKE '%vacina contra a febre amarela%'
                OR REGEXP_REPLACE(NORMALIZE(descricao, NFD), r'\p{Mn}', '') LIKE '%vacina triplice viral  sarampo  caxumba  rubeola   atenuada%'
            )
)

SELECT
    cns as cns_paciente,
    dt_nascimento as data_nascimento_paciente,
    ordem,
    idade_meses,
    aplicacao_data,
    aplicacao_data_f,
    aplicacao_meses,
    aplicacao_dias,
    CONCAT('VALIDA_DATA_APLICACAO_',UPPER(vacina)) AS vacina_formatada,
    dose,
    dose_f
FROM
    vacinas_detectadas,
    UNNEST(vacinas) AS vacina
WHERE
    vacina IS NOT NULL
ORDER BY
    vacina_formatada, ordem