{{
    config(
        schema="brutos_sheets",
        alias="municipios_brasil",
        materialized="table",
        tags=["monthly"],
    )
}}

with
    source as (
        select
            {{ process_null("cod_uf") }} as cod_uf,
            {{ process_null("nome_uf") }} as nome_uf,
            {{ process_null("cod_mun") }} as cod_mun,
            {{ process_null("nome_mun") }} as nome_mun,
            {{ process_null("data_atualizacao") }} as data_atualizacao,
        from {{ source("brutos_sheets_staging", "municipios_brasil") }}
    )

select *
from source
