{{
    config(
        alias="diarios_rj",
        materialized="table",
        partition_by={
            "field": "data_particao",
            "data_type": "date"
        },
    )
}}

with
    preprocessed as (
        select
            {{ process_null(clean_numeric_string('do_id')) }} as id_diario,
            {{ process_null('do_data') }} as data_publicacao,
            trim({{ process_null('titulo') }}) as titulo,
            trim({{ process_null('texto') }}) as texto,
            trim({{ process_null('html') }}) as html,
            _extracted_at as data_extracao,
            ano_particao,
            mes_particao,
            data_particao
        from {{ source("brutos_diario_oficial_staging", "diarios_municipio") }}
    ),
    deduplicated as (
        select *
        from preprocessed
        qualify row_number() over (partition by titulo, id_diario) = 1
    ),
    typed as (
        select
            cast(id_diario as INT64) as id_diario,
            DATE(data_publicacao) as data_publicacao,
            titulo,
            texto,
            html,

            -- (0) Em `data_extracao`, recebemos uma string como '2025-06-04 13:37:04.182894-03:00'
            --     Mas DATETIME no BigQuery não tem informação de fusos
            -- (2) Depois, fazemos cast para DATETIME. Isso nos retorna uma data/hora
            --     sem informação de fuso, convertida para UTC; i.e. soma 3 à hora
            DATETIME(
                -- (1) Primeiro lemos a string como TIMESTAMP, que possui fuso
                -- [Ref] https://cloud.google.com/bigquery/docs/reference/standard-sql/format-elements#format_elements_date_time
                PARSE_TIMESTAMP(
                    '%Y-%m-%d %H:%M:%E*S%Ez',
                    data_extracao
                ),
                -- (3) Contudo, passamos a informação de fuso na conversão. O resultado final
                --     é, efetivamente, só a remoção do '-03:00', mas agora o tipo está correto
                "America/Sao_Paulo"
            ) as data_extracao,

            cast(ano_particao as INT64) as ano_particao,
            cast(mes_particao as INT64) as mes_particao,
            cast(data_particao as date) as data_particao
        from deduplicated
    )
select *
from typed
