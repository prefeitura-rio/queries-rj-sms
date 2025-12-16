{{
    config(
        alias="solicitacao_saude_bucal",
        materialized="table"
    )
}}

with
    source as (
        select
            *
        from {{ source("brutos_informes_vitacare_staging", "solicitacao_saude_bucal") }}
    ),

    sem_duplicatas as (
        select *
        from source
        qualify row_number() over (
            partition by _source_file, indice
            order by _loaded_at desc
        ) = 1
    ),

    extrair_informacoes as (
        select
            {{ process_null("ap") }} as ap,
            {{ process_null("cnes") }} as cnes,
            {{ process_null("unidade") }} as unidade,
            {{ process_null("cod_equipe") }} as cod_equipe,
            {{ process_null("equipe") }} as equipe,
            {{ process_null("microarea") }} as microarea,
            {{ process_null("paciente") }} as nome,
            {{ process_null("n_pront") }} as n_prontuario,
            {{ process_null("gestante") }} as gestante,
            {{ process_null("has") }} as has,
            {{ process_null("dm") }} as dm,
            {{ process_null("hiv") }} as hiv,
            {{ process_null("tb") }} as tb,
            {{ process_null("tabagista") }} as tabagista,
            {{ process_null("familia_recebe_bf") }} as familia_recebe_bf,

            cast(
                {{ process_null("data_ultima_cons_dentista") }}
                as date format "DD/MM/YYYY"
            ) as data_ultima_cons_dentista,
            cast(
                {{ process_null("data_ultima_cons_nasf") }}
                as date format "DD/MM/YYYY"
            ) as data_ultima_cons_nasf,

            struct(
                _source_file as arquivo_fonte,
                safe_cast(REGEXP_EXTRACT(_source_file, r'/(\d{4}-\d{2}-\d{2})/') as timestamp) as criado_em,
                safe_cast(_extracted_at as timestamp) as extraido_em,
                safe_cast(_loaded_at as timestamp) as carregado_em
            ) as metadados,

            {{ process_null("ano_particao") }} as ano_particao,
            {{ process_null("mes_particao") }} as mes_particao,
            {{ process_null("data_particao") }} as data_particao

        from sem_duplicatas
    )
select *
from extrair_informacoes
