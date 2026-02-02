{{
    config(
        alias        = "sinanrio__tb_investiga",
        materialized = "table",
        tags         = ["subpav", "sinanrio"],
        cluster_by   = ["nu_notificacao", "dt_notificacao", "co_cid", "co_municipio_notificacao"]
    )
}}

with
    source as (
        select *
        from {{ source("brutos_plataforma_subpav_staging", "subpav_sinanrio__tb_investiga") }}
    ),

    sem_duplicatas as (
        select *
        from source
        qualify
            row_number() over (
                partition by nu_notificacao, dt_notificacao, co_cid, co_municipio_notificacao
                order by coalesce(dt_notificacao, datalake_loaded_at) desc
            ) = 1
    ),

    extrair_informacoes as (
        select
            {{ process_null('nu_notificacao') }} AS nu_notificacao,
            SAFE_CAST({{ process_null('dt_notificacao') }} AS DATE) AS dt_notificacao,
            {{ process_null('co_cid') }} AS co_cid,
            {{ process_null('co_municipio_notificacao') }} AS co_municipio_notificacao,

            {{ process_null('nu_prontuario') }} AS nu_prontuario,
            {{ process_null('tp_entrada') }} AS tp_entrada,
            {{ process_null('tp_institucionalizado') }} AS tp_institucionalizado,
            {{ process_null('tp_raio_x') }} AS tp_raio_x,
            {{ process_null('tp_turberculinico') }} AS tp_turberculinico,
            {{ process_null('tp_forma') }} AS tp_forma,
            {{ process_null('tp_extrapulmonar_1') }} AS tp_extrapulmonar_1,
            {{ process_null('tp_extrapulmonar_2') }} AS tp_extrapulmonar_2,
            {{ process_null('ds_extrapulmonar_outro') }} AS ds_extrapulmonar_outro,

            {{ process_null('st_agravo_aids') }} AS st_agravo_aids,
            {{ process_null('st_agravo_alcolismo') }} AS st_agravo_alcolismo,
            {{ process_null('st_agravo_diabete') }} AS st_agravo_diabete,
            {{ process_null('st_agravo_mental') }} AS st_agravo_mental,
            {{ process_null('st_agravo_outro') }} AS st_agravo_outro,
            {{ process_null('ds_agravo_outro') }} AS ds_agravo_outro,

            {{ process_null('st_baciloscopia_escarro') }} AS st_baciloscopia_escarro,
            {{ process_null('st_baciloscopia_outro') }} AS st_baciloscopia_outro,
            {{ process_null('st_baciloscopia_escarro2') }} AS st_baciloscopia_escarro2,
            {{ process_null('tp_cultura_escarro') }} AS tp_cultura_escarro,
            {{ process_null('tp_cultura_outro') }} AS tp_cultura_outro,

            {{ process_null('tp_hiv') }} AS tp_hiv,
            {{ process_null('tp_histopatologia') }} AS tp_histopatologia,

            SAFE_CAST({{ process_null('dt_inicio_tratamento') }} AS DATE) AS dt_inicio_tratamento,

            {{ process_null('st_droga_rifampicina') }} AS st_droga_rifampicina,
            {{ process_null('st_droga_isoniazida') }} AS st_droga_isoniazida,
            {{ process_null('st_droga_etambutol') }} AS st_droga_etambutol,
            {{ process_null('st_droga_estreptomicina') }} AS st_droga_estreptomicina,
            {{ process_null('st_droga_pirazinamida') }} AS st_droga_pirazinamida,
            {{ process_null('st_droga_etionamida') }} AS st_droga_etionamida,
            {{ process_null('st_droga_outro') }} AS st_droga_outro,
            {{ process_null('ds_droga_outro') }} AS ds_droga_outro,

            {{ process_null('tp_tratamento') }} AS tp_tratamento,
            SAFE_CAST({{ process_null('nu_contato') }} AS INT64) AS nu_contato,
            {{ process_null('co_uf_atual') }} AS co_uf_atual,
            {{ process_null('co_municipio_atual') }} AS co_municipio_atual,
            {{ process_null('nu_notificacao_atual') }} AS nu_notificacao_atual,
            SAFE_CAST({{ process_null('dt_notificacao_atual') }} AS DATE) AS dt_notificacao_atual,
            SAFE_CAST({{ process_null('co_unidade_saude_atual') }} AS NUMERIC) AS co_unidade_saude_atual,

            {{ process_null('st_baciloscopia_2_mes') }} AS st_baciloscopia_2_mes,
            {{ process_null('st_baciloscopia_4_mes') }} AS st_baciloscopia_4_mes,
            {{ process_null('st_baciloscopia_6_mes') }} AS st_baciloscopia_6_mes,
            {{ process_null('st_baciloscopia_1_mes') }} AS st_baciloscopia_1_mes,
            {{ process_null('st_baciloscopia_3_mes') }} AS st_baciloscopia_3_mes,
            {{ process_null('st_baciloscopia_5_mes') }} AS st_baciloscopia_5_mes,

            {{ process_null('tp_tratamento_acompanhamento') }} AS tp_tratamento_acompanhamento,
            SAFE_CAST({{ process_null('dt_mudanca_tratamento') }} AS DATE) AS dt_mudanca_tratamento,
            SAFE_CAST({{ process_null('nu_contato_examinado') }} AS NUMERIC) AS nu_contato_examinado,

            {{ process_null('tp_situacao_mes_9') }} AS tp_situacao_mes_9,
            {{ process_null('tp_situacao_mes_12') }} AS tp_situacao_mes_12,
            {{ process_null('tp_situacao_encerramento') }} AS tp_situacao_encerramento,

            {{ process_null('co_uf_residencia_atual') }} AS co_uf_residencia_atual,
            {{ process_null('co_municipio_residencia_atual') }} AS co_municipio_residencia_atual,
            SAFE_CAST({{ process_null('co_bairro_residencia_atual') }} AS NUMERIC) AS co_bairro_residencia_atual,
            SAFE_CAST({{ process_null('co_distrito_residencia_atual') }} AS NUMERIC) AS co_distrito_residencia_atual,
            {{ process_null('no_bairro_residencia_atual') }} AS no_bairro_residencia_atual,
            {{ process_null('nu_cep_residencia_atual') }} AS nu_cep_residencia_atual,           
            SAFE_CAST({{ process_null('dt_encerramento') }} AS DATE) AS dt_encerramento,
            {{ process_null('tp_pcr_escarro') }} AS tp_pcr_escarro,
            {{ process_null('tp_pop_liberdade') }} AS tp_pop_liberdade,
            {{ process_null('tp_pop_rua') }} AS tp_pop_rua,
            {{ process_null('tp_pop_saude') }} AS tp_pop_saude,
            {{ process_null('tp_pop_imigrante') }} AS tp_pop_imigrante,
            {{ process_null('tp_benef_gov') }} AS tp_benef_gov,
            {{ process_null('st_agravo_drogas') }} AS st_agravo_drogas,
            {{ process_null('st_agravo_tabaco') }} AS st_agravo_tabaco,
            {{ process_null('tp_molecular') }} AS tp_molecular,
            {{ process_null('tp_sensibilidade') }} AS tp_sensibilidade,
            SAFE_CAST({{ process_null('nu_contato_identificados') }} AS NUMERIC) AS nu_contato_identificados,
            {{ process_null('tp_antirretroviral_trat') }} AS tp_antirretroviral_trat,
            {{ process_null('st_bacil_apos_6_mes') }} AS st_bacil_apos_6_mes,
            {{ process_null('nu_prontuario_atual') }} AS nu_prontuario_atual,
            {{ process_null('tp_transf') }} AS tp_transf,
            {{ process_null('co_uf_transf') }} AS co_uf_transf,
            {{ process_null('co_municipio_transf') }} AS co_municipio_transf,

            SAFE_CAST({{ process_null('datalake_loaded_at') }} AS TIMESTAMP) AS datalake_loaded_at
        from sem_duplicatas
    )

select *
from extrair_informacoes
