{{ 
config(
    alias        = "sinanrio_legado__notificacao",
    materialized = "table",
    tags         = ["subpav", "sinan_legado", "sinanrio"],
    cluster_by   = ["nu_notificacao", "dt_notificacao", "co_cid", "co_municipio_notificacao"]
) 
}}

with
source as (
    select *
    from {{ source("brutos_plataforma_subpav_staging", "subpav_sinan__tuberculose_sinan") }}
),

sem_duplicatas as (
    select *
    from source
    qualify
        row_number() over (
            partition by nu_notificacao, dt_notificacao, co_cid, co_municipio_notificacao
            order by coalesce(timestamp, dt_notificacao, datalake_loaded_at) desc
        ) = 1
),

extrair_informacoes as (
        select
            SAFE_CAST({{ process_null('id') }} AS INT64) AS id,

            {{ process_null('nu_notificacao') }} AS nu_notificacao,
            SAFE_CAST({{ process_null('dt_notificacao') }} AS DATE) AS dt_notificacao,
            {{ process_null('co_cid') }} AS co_cid,
            {{ process_null('co_municipio_notificacao') }} AS co_municipio_notificacao,
            SAFE_CAST({{ process_null('co_unidade_notificacao') }} AS INT64) AS co_unidade_notificacao,
            {{ process_null('co_uf_notificacao') }} AS co_uf_notificacao,
            {{ process_null('tp_notificacao') }} AS tp_notificacao,
            SAFE_CAST({{ process_null('dt_diagnostico_sintoma') }} AS DATE) AS dt_diagnostico_sintoma,

            {{ process_null('no_nome_paciente') }} AS no_nome_paciente,
            SAFE_CAST({{ process_null('dt_nascimento') }} AS DATE) AS dt_nascimento,
            SAFE_CAST({{ process_null('nu_idade') }} AS INT64) AS nu_idade,
            {{ process_null('tp_sexo') }} AS tp_sexo,
            {{ process_null('tp_gestante') }} AS tp_gestante,
            {{ process_null('tp_raca_cor') }} AS tp_raca_cor,
            {{ process_null('tp_escolaridade') }} AS tp_escolaridade,
            {{ process_null('nu_cartao_sus') }} AS nu_cartao_sus,
            {{ process_null('no_nome_mae') }} AS no_nome_mae,

            {{ process_null('co_uf_residencia') }} AS co_uf_residencia,
            {{ process_null('co_municipio_residencia') }} AS co_municipio_residencia,
            {{ process_null('co_distrito_residencia') }} AS co_distrito_residencia,
            SAFE_CAST({{ process_null('co_bairro_residencia') }} AS INT64) AS co_bairro_residencia,
            {{ process_null('no_bairro_residencia') }} AS no_bairro_residencia,
            {{ process_null('nu_cep_residencia') }} AS nu_cep_residencia,
            SAFE_CAST({{ process_null('co_geo_campo_1') }} AS INT64) AS co_geo_campo_1,
            SAFE_CAST({{ process_null('co_geo_campo_2') }} AS INT64) AS co_geo_campo_2,
            SAFE_CAST({{ process_null('co_logradouro_residencia') }} AS INT64) AS co_logradouro_residencia,
            {{ process_null('no_logradouro_residencia') }} AS no_logradouro_residencia,
            {{ process_null('nu_residencia') }} AS nu_residencia,
            {{ process_null('ds_complemento_residencia') }} AS ds_complemento_residencia,
            {{ process_null('ds_referencia_residencia') }} AS ds_referencia_residencia,
            {{ process_null('nu_ddd_residencia') }} AS nu_ddd_residencia,
            {{ process_null('nu_telefone_residencia') }} AS nu_telefone_residencia,
            {{ process_null('tp_zona_residencia') }} AS tp_zona_residencia,
            {{ process_null('co_pais_residencia') }} AS co_pais_residencia,

            SAFE_CAST({{ process_null('dt_investigacao') }} AS DATE) AS dt_investigacao,
            SAFE_CAST({{ process_null('co_cbo_ocupacao') }} AS NUMERIC) AS co_cbo_ocupacao,
            {{ process_null('tp_classificacao_final') }} AS tp_classificacao_final,
            {{ process_null('ds_classificacao_final') }} AS ds_classificacao_final,
            {{ process_null('tp_criterio_confirmacao') }} AS tp_criterio_confirmacao,
            {{ process_null('tp_modo_infeccao') }} AS tp_modo_infeccao,
            {{ process_null('ds_modo_infeccao_outro') }} AS ds_modo_infeccao_outro,
            {{ process_null('tp_local_infeccao') }} AS tp_local_infeccao,
            {{ process_null('ds_local_infeccao_outro') }} AS ds_local_infeccao_outro,
            {{ process_null('tp_autoctone_residencia') }} AS tp_autoctone_residencia,

            {{ process_null('co_uf_infeccao') }} AS co_uf_infeccao,
            SAFE_CAST({{ process_null('co_pais_infeccao') }} AS INT64) AS co_pais_infeccao,
            {{ process_null('co_municipio_infeccao') }} AS co_municipio_infeccao,
            {{ process_null('co_distrito_infeccao') }} AS co_distrito_infeccao,
            SAFE_CAST({{ process_null('co_bairro_infeccao') }} AS INT64) AS co_bairro_infeccao,
            {{ process_null('no_bairro_infeccao') }} AS no_bairro_infeccao,
            SAFE_CAST({{ process_null('co_unidade_infeccao') }} AS INT64) AS co_unidade_infeccao,
            {{ process_null('no_localidade_infeccao') }} AS no_localidade_infeccao,

            {{ process_null('st_doenca_trabalho') }} AS st_doenca_trabalho,
            {{ process_null('tp_evolucao_caso') }} AS tp_evolucao_caso,
            {{ process_null('ds_evolucao_caso_outro') }} AS ds_evolucao_caso_outro,
            SAFE_CAST({{ process_null('dt_obito') }} AS DATE) AS dt_obito,
            SAFE_CAST({{ process_null('dt_encerramento') }} AS DATE) AS dt_encerramento,

            {{ process_null('ds_semana_notificacao') }} AS ds_semana_notificacao,
            {{ process_null('ds_semana_sintoma') }} AS ds_semana_sintoma,
            {{ process_null('ds_chave_fonetica') }} AS ds_chave_fonetica,
            {{ process_null('ds_soundex') }} AS ds_soundex,

            SAFE_CAST({{ process_null('dt_digitacao') }} AS DATE) AS dt_digitacao,
            SAFE_CAST({{ process_null('dt_transf_us') }} AS DATE) AS dt_transf_us,
            SAFE_CAST({{ process_null('dt_transf_dm') }} AS DATE) AS dt_transf_dm,
            SAFE_CAST({{ process_null('dt_transf_sm') }} AS DATE) AS dt_transf_sm,
            SAFE_CAST({{ process_null('dt_transf_rm') }} AS DATE) AS dt_transf_rm,
            SAFE_CAST({{ process_null('dt_transf_rs') }} AS DATE) AS dt_transf_rs,
            SAFE_CAST({{ process_null('dt_transf_se') }} AS DATE) AS dt_transf_se,

            {{ process_null('nu_lote_vertical') }} AS nu_lote_vertical,
            {{ process_null('nu_lote_horizontal') }} AS nu_lote_horizontal,
            {{ process_null('tp_duplicidade') }} AS tp_duplicidade,
            {{ process_null('tp_suspeita') }} AS tp_suspeita,
            {{ process_null('st_vincula') }} AS st_vincula,
            {{ process_null('tp_fluxo_retorno') }} AS tp_fluxo_retorno,
            {{ process_null('st_modo_transmissao') }} AS st_modo_transmissao,
            {{ process_null('tp_veiculo_transmissao') }} AS tp_veiculo_transmissao,
            {{ process_null('ds_veiculo_transmissao_outro') }} AS ds_veiculo_transmissao_outro,
            {{ process_null('tp_local_surto') }} AS tp_local_surto,
            {{ process_null('ds_local_outro') }} AS ds_local_outro,

            SAFE_CAST({{ process_null('nu_caso_suspeito') }} AS INT64) AS nu_caso_suspeito,
            {{ process_null('tp_inquerito') }} AS tp_inquerito,
            SAFE_CAST({{ process_null('nu_caso_examinado') }} AS INT64) AS nu_caso_examinado,
            SAFE_CAST({{ process_null('nu_caso_positivo') }} AS INT64) AS nu_caso_positivo,

            {{ process_null('ds_observacao') }} AS ds_observacao,
            {{ process_null('tp_delimitacao_surto') }} AS tp_delimitacao_surto,
            {{ process_null('ds_delimitacao_surto_outro') }} AS ds_delimitacao_surto_outro,
            {{ process_null('st_fluxo_retorno_recebido') }} AS st_fluxo_retorno_recebido,

            {{ process_null('ds_identificador_registro') }} AS ds_identificador_registro,
            {{ process_null('st_importado') }} AS st_importado,
            {{ process_null('st_criptografia') }} AS st_criptografia,
            {{ process_null('tp_sistema') }} AS tp_sistema,
            {{ process_null('st_espelho') }} AS st_espelho,

            {{ process_null('ts_codigo1') }} AS ts_codigo1,
            {{ process_null('ts_codigo2') }} AS ts_codigo2,

            SAFE_CAST({{ process_null('tp_unidad_notificadora_externa') }} AS NUMERIC) AS tp_unidad_notificadora_externa,
            {{ process_null('co_unidad_notificadora_externa') }} AS co_unidad_notificadora_externa,

            {{ process_null('cpf_notificante') }} AS cpf_notificante,
            {{ process_null('cpf_paciente') }} AS cpf_paciente,
            {{ process_null('dnv_paciente') }} AS dnv_paciente,
            {{ process_null('justificativa') }} AS justificativa,

            {{ process_null('lat') }} AS lat,
            {{ process_null('lng') }} AS long,
            {{ process_null('notif_assistente') }} AS notif_assistente,
            {{ process_null('resp_encerramento') }} AS resp_encerramento,

            SAFE_CAST({{ process_null('st_agravo_tabagismo') }} AS INT64) AS st_agravo_tabagismo,
            SAFE_CAST({{ process_null('st_pcr_escarro') }} AS INT64) AS st_pcr_escarro,
            {{ process_null('tp_cultura_justificativa') }} AS tp_cultura_justificativa,

            SAFE_CAST({{ process_null('visivel') }} AS INT64) AS visivel,
            SAFE_CAST({{ process_null('finalizado') }} AS INT64) AS finalizado,
            SAFE_CAST({{ process_null('timestamp') }} AS TIMESTAMP) AS timestamp,
            SAFE_CAST({{ process_null('datalake_loaded_at') }} AS TIMESTAMP) AS datalake_loaded_at
    from sem_duplicatas
)

select *
from extrair_informacoes
