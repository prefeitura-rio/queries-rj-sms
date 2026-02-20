{{
    config(
        alias="diarios_rj",
        materialized="table",
        tags=["cdi_vps"],
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
            {{ process_null(clean_numeric_string('materia_id')) }} as id_materia,
            {{ process_null('do_data') }} as data_publicacao,
            {{ process_null(clean_numeric_string('secao_indice')) }} as secao_indice,
            {{ process_null(clean_numeric_string('bloco_indice')) }} as bloco_indice,
            {{ process_null(clean_numeric_string('conteudo_indice')) }} as conteudo_indice,
            {{ process_null('trim(secao)') }} as pasta,
            {{ process_null('trim(titulo)') }} as arquivo,
            {{ process_null('trim(cabecalho)') }} as cabecalho,
            {{ process_null('trim(conteudo)') }} as conteudo,
            _extracted_at as data_extracao,
            ano_particao,
            mes_particao,
            data_particao
        from {{ source("brutos_diario_oficial_staging", "diarios_municipio") }}
    ),
    deduplicated as (
        select *
        from preprocessed
        qualify row_number() over (partition by arquivo, id_diario, secao_indice,bloco_indice,conteudo_indice  order by data_particao desc) = 1
    ),
    typed as (
        select
            cast(id_diario as STRING) as id_diario,
            cast(id_materia as STRING) as id_materia,
            DATE(data_publicacao) as data_publicacao,
            cast(secao_indice as INT64) as secao_indice,
            cast(bloco_indice as INT64) as bloco_indice,
            cast(conteudo_indice as INT64) as conteudo_indice,
            pasta,
            arquivo,
            cabecalho,
            conteudo,

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
            DATE(data_particao) as data_particao
        from deduplicated
    )
select *
from typed
