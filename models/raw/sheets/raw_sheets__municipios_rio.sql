{{ config(schema="brutos_sheets", alias="municipios_rio", materialized="table") }}

with
    source as (
        select
            cod_ibge as cod_ibge_7,
            left(cod_ibge, 6) as cod_ibge_6,
            upper(trim(mun_nome)) as nome_municipio

        from {{ source("brutos_sheets_staging", "municipios_rio") }}
    )

select *
from source
