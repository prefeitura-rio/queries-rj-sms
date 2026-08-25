{{
    config(
        schema="brutos_sheets",
        alias="depara_vacinas",
        tags=["monthly", "vacinacao"],
        meta={
            "owner": "daniel",
            "team": "cit"
        }
    )
}}

-- De/para criado para padronizar nomes das vacinas de diversas fontes

with
    source as (
        select *
        from {{ source("brutos_sheets_staging", "depara_vacinas") }}
    )
select
    lower(nome_original) as nome_original,
    nome_padronizado,
    codigo_sipni
from source
