{{ config(schema="brutos_centralderegulacao_mysql", alias="tea_relatorio") }}

with
    source as (
        select *
        from {{ source("brutos_centralderegulacao_mysql_staging", "vw_tea_relatorio") }}
    ),
    deduplicated as (
        select *
        from source
        qualify row_number() over (partition by idusuario order by solicitacaodatahora desc) = 1
    )
select *
from deduplicated
