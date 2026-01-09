{{
    config(
        schema="brutos_sheets",
        alias="vacinas_padronizadas",
        tags=["weekly"],
    )
}}

with source as (
    select

        {{ process_null("nome_de") }} as nome_de,
        -- Caso não tenha uma correspondência, usa `nome_de`
        -- com tratamento/capitalização apropriada
        coalesce(
            {{ process_null("nome_para") }},
            {{ proper_br(process_null("nome_de")) }}
        ) as nome_para,

        {{ process_null("sigla") }} as sigla,
        {{ process_null("detalhes") }} as detalhes,
        {{ process_null("categoria") }} as categoria

    from {{ source("brutos_sheets_staging", "vacinas_padronizadas") }}
)

select *
from source
where nome_de is not null
