{{ config(schema="brutos_centralderegulacao_mysql", alias="tea_relatorio") }}

with
    source as (
        select *
        from {{ source("brutos_centralderegulacao_mysql_staging", "vw_tea_relatorio") }}
    ),
    final as (select * from source)
select *
from final
