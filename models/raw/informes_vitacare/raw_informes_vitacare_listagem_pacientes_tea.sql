{{
    config(
        alias="listagem_pacientes_tea",
        materialized="table",
        tags=["subpav", "tea"]
    )
}}

WITH source AS (
    SELECT *
    FROM {{ source("brutos_informes_vitacare_staging", "listagem_pacientes_tea") }}
),

sem_duplicatas as (
    select *
    from source
    qualify row_number() over (partition by _source_file, indice order by _loaded_at desc) = 1 
),

extrair_informacoes AS (
    SELECT
        -- Chave de competência (AAAA-MM) extraída do _source_file
        {{ extract_competencia_from_path('_source_file') }}       AS competencia,

        -- Identificadores principais
        {{ process_null('acto_id') }}                             AS acto_id,
        {{ process_null('ut_id') }}                               AS ut_id,
        {{ process_null('ap') }}                                  AS ap,

        -- Unidade / Profissional
        {{ process_null('numero_cnes_da_unidade') }}              AS numero_cnes_da_unidade,
        {{ process_null('unidade') }}                             AS unidade,
        {{ process_null('numero_cns_profissional') }}             AS numero_cns_profissional,
        {{ process_null('tipo_atendimento') }}                    AS tipo_atendimento,
        {{ process_null('nome_profissional') }}                   AS nome_profissional,

        -- Datas principais
        {{ parse_date('data_consulta') }}                         AS data_consulta,
        {{ parse_date('data_nasc_paciente') }}                    AS data_nasc_paciente,

        -- Paciente
        {{ process_null('nome_paciente') }}                       AS nome_paciente,
        {{ process_null('cpf_paciente') }}                        AS cpf_paciente,
        {{ validate_cpf('cpf_paciente') }}                        AS cpf_paciente_valido,
        {{ process_null('cns_paciente') }}                        AS cns_paciente,
        {{ process_null('sexo_paciente') }}                       AS sexo_paciente,

        -- CID‑10 (até 5 diagnósticos)
        {{ process_null('cid_consulta_diagnostico_01') }}         AS cid_consulta_diagnostico_01,
        {{ process_null('cid_01') }}                              AS cid_01,
        {{ process_null('estado_cid_consulta_01') }}              AS estado_cid_consulta_01,
        {{ process_null('cid_consulta_diagnostico_02') }}         AS cid_consulta_diagnostico_02,
        {{ process_null('cid_02') }}                              AS cid_02,
        {{ process_null('estado_cid_consulta_02') }}              AS estado_cid_consulta_02,
        {{ process_null('cid_consulta_diagnostico_03') }}         AS cid_consulta_diagnostico_03,
        {{ process_null('cid_03') }}                              AS cid_03,
        {{ process_null('estado_cid_consulta_03') }}              AS estado_cid_consulta_03,
        {{ process_null('cid_consulta_diagnostico_04') }}         AS cid_consulta_diagnostico_04,
        {{ process_null('cid_04') }}                              AS cid_04,
        {{ process_null('estado_cid_consulta_04') }}              AS estado_cid_consulta_04,
        {{ process_null('cid_consulta_diagnostico_05') }}         AS cid_consulta_diagnostico_05,
        {{ process_null('cid_05') }}                              AS cid_05,
        {{ process_null('estado_cid_consulta_05') }}              AS estado_cid_consulta_05,

        -- Escala M‑CHAT‑R/F (21 perguntas + score/classificação)
        {{ process_null('m_chat_se_voce_apontar_algum') }}         AS m_chat_se_voce_apontar_algum,
        {{ process_null('m_chat_alguma_vez_voce_se_perg') }}      AS m_chat_alguma_vez_voce_se_perg,
        {{ process_null('m_chat_crianca_brinca_faz_cont') }}      AS m_chat_crianca_brinca_faz_cont,
        {{ process_null('m_chat_crianca_gosta_subir_nas') }}      AS m_chat_crianca_gosta_subir_nas,
        {{ process_null('m_chat_crianca_faz_movimentos') }}       AS m_chat_crianca_faz_movimentos,
        {{ process_null('m_chat_crianca_aponta_dedo') }}          AS m_chat_crianca_aponta_dedo,
        {{ process_null('m_chat_crianca_aponta_dedo_2') }}        AS m_chat_crianca_aponta_dedo_2,
        {{ process_null('m_chat_crianca_se_interessa') }}         AS m_chat_crianca_se_interessa,
        {{ process_null('m_chat_crianca_traz_coisas') }}          AS m_chat_crianca_traz_coisas,
        {{ process_null('m_chat_crianca_responde_voce') }}        AS m_chat_crianca_responde_voce,
        {{ process_null('m_chat_voce_sorri_crianca_ela') }}       AS m_chat_voce_sorri_crianca_ela,
        {{ process_null('m_chat_crianca_fica_muito_inco') }}      AS m_chat_crianca_fica_muito_inco,
        {{ process_null('m_chat_r_f_a_crianca_anda') }}           AS m_chat_r_f_a_crianca_anda,
        {{ process_null('m_chat_crianca_olha_nos_seus') }}        AS m_chat_crianca_olha_nos_seus,
        {{ process_null('m_chat_crianca_tenta_imitar') }}         AS m_chat_crianca_tenta_imitar,
        {{ process_null('m_chat_voce_vira_cabeca_olhar') }}       AS m_chat_voce_vira_cabeca_olhar,
        {{ process_null('m_chat_crianca_tenta_fazer') }}          AS m_chat_crianca_tenta_fazer,
        {{ process_null('m_chat_crianca_compreende_voce') }}      AS m_chat_crianca_compreende_voce,
        {{ process_null('m_chat_acontece_algo_novo_olha') }}      AS m_chat_acontece_algo_novo_olha,
        {{ process_null('m_chat_crianca_gosta_atividade') }}      AS m_chat_crianca_gosta_atividade,
        {{ process_null('m_chat_r_f_score') }}                    AS m_chat_r_f_score,
        {{ process_null('m_chat_r_f_classificacao') }}            AS m_chat_r_f_classificacao,

        -- Escala AQ‑10 (10 perguntas + score/classificação)
        {{ process_null('aq_10_elea_frequentemente_perc') }}       AS aq_10_elea_frequentemente_perc,
        {{ process_null('aq_10_normalmente_elea_foca') }}         AS aq_10_normalmente_elea_foca,
        {{ process_null('aq_10_em_grupos_sociais_elea') }}        AS aq_10_em_grupos_sociais_elea,
        {{ process_null('aq_10_elea_consegue_facilmente') }}      AS aq_10_elea_consegue_facilmente,
        {{ process_null('aq_10_elea_nao_sabe_como_mante') }}       AS aq_10_elea_nao_sabe_como_mante,
        {{ process_null('aq_10_socialmente_elea_convers') }}      AS aq_10_socialmente_elea_convers,
        {{ process_null('aq_10_ao_ouvir_ler_uma_histori') }}      AS aq_10_ao_ouvir_ler_uma_histori,
        {{ process_null('aq_10_na_pre_escola_elea_gosta') }}      AS aq_10_na_pre_escola_elea_gosta,
        {{ process_null('aq_10_elea_consegue_entender') }}        AS aq_10_elea_consegue_entender,
        {{ process_null('aq_10_elea_tem_dificuldades') }}         AS aq_10_elea_tem_dificuldades,
        {{ process_null('aq_10_score') }}                         AS aq_10_score,
        {{ process_null('aq_10_classificacao') }}                 AS aq_10_classificacao,

        -- Metadados do carregamento
        STRUCT(
            _source_file                                                    AS arquivo_fonte,
            SAFE_CAST(REGEXP_EXTRACT(_source_file, r"/(\d{4}-\d{2}-\d{2})/") AS TIMESTAMP) AS criado_em,
            SAFE_CAST(_extracted_at AS TIMESTAMP)                           AS extraido_em,
            SAFE_CAST(_loaded_at AS TIMESTAMP)                              AS carregado_em
        ) AS metadados,

        -- Particionamento
        {{ process_null('ano_particao') }}                                   AS ano_particao,
        {{ process_null('mes_particao') }}                                   AS mes_particao,
        {{ parse_date('data_particao') }}                                    AS data_particao
    FROM sem_duplicatas
)

SELECT *
FROM extrair_informacoes
