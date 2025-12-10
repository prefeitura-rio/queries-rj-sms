{{
    config(
        alias        = "sinanrio__tb_sintomatico",
        materialized = "table",
        tags         = ["subpav", "sinanrio"],
        cluster_by   = ["id_sintomatico", "cpf", "cns"]
    )
}}

with
    source as (
        select *
        from {{ source("brutos_plataforma_subpav_staging", "subpav_sinanrio__tb_sintomatico") }}
    ),

    sem_duplicatas as (
        select *
        from source
        qualify
            row_number() over (
                partition by id_sintomatico
                order by coalesce(created_at, datalake_loaded_at) desc
            ) = 1
    ),

    extrair_informacoes as (
        select
            SAFE_CAST({{ process_null('id_sintomatico') }} AS INT64)    AS id_sintomatico,
            {{ process_null('cpf') }}                                   AS cpf,
            {{ process_null('cns') }}                                   AS cns,
            {{ process_null('nome') }}                                  AS nome,
            SAFE_CAST({{ process_null('dt_nascimento') }} AS DATE)      AS dt_nascimento,
            SAFE_CAST({{ process_null('id_raca_cor') }} AS INT64)       AS id_raca_cor,
            SAFE_CAST({{ process_null('id_sexo') }} AS INT64)           AS id_sexo,
            SAFE_CAST({{ process_null('id_escolaridade') }} AS INT64)   AS id_escolaridade,
            {{ process_null('telefone') }}                              AS telefone,
            {{ process_null('cep') }}                                   AS cep,
            {{ process_null('logradouro') }}                            AS logradouro,
            {{ process_null('numero') }}                                AS numero,
            {{ process_null('complemento') }}                           AS complemento,
            SAFE_CAST({{ process_null('id_bairro') }} AS INT64)         AS id_bairro,
            {{ process_null('cidade') }}                                AS cidade,
            {{ process_null('cnes') }}                                  AS cnes,
            {{ process_null('ine') }}                                   AS ine,
            SAFE_CAST({{ process_null('nao_municipe') }} AS INT64)      AS nao_municipe,
            {{ process_null('n_prontuario') }}                          AS n_prontuario,
            {{ process_null('n_sinan') }}                               AS n_sinan,

            SAFE_CAST({{ process_null('id_bac_1') }} AS INT64)          AS id_bac_1,
            SAFE_CAST({{ process_null('dt_bac_1') }} AS DATE)           AS dt_bac_1,
            SAFE_CAST({{ process_null('id_bac_2') }} AS INT64)          AS id_bac_2,
            SAFE_CAST({{ process_null('dt_bac_2') }} AS DATE)           AS dt_bac_2,
            SAFE_CAST({{ process_null('id_trmtb') }} AS INT64)          AS id_trmtb,
            SAFE_CAST({{ process_null('dt_trmtb') }} AS DATE)           AS dt_trmtb,
            SAFE_CAST({{ process_null('id_rx_torax') }} AS INT64)       AS id_rx_torax,

            {{ process_null('obs') }}                                   AS obs,
            {{ process_null('cnes_cadastrante') }}                      AS cnes_cadastrante,
            {{ process_null('cpf_cadastrante') }}                       AS cpf_cadastrante,
            {{ process_null('cns_cadastrante') }}                       AS cns_cadastrante,
            SAFE_CAST({{ process_null('id_tb_situacao') }} AS INT64)    AS id_tb_situacao,
            {{ process_null('origem') }}                                AS origem,

            SAFE_CAST({{ process_null('created_at') }} AS TIMESTAMP)    AS created_at,
            SAFE_CAST({{ process_null('datalake_loaded_at') }} AS TIMESTAMP) AS datalake_loaded_at
        from sem_duplicatas
    )

select *
from extrair_informacoes
