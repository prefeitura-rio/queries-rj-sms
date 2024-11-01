{{
    config(
        schema="brutos_sheets",
        alias="profissionais_cns_cpf_aux",
    )
}}

with
    source as (
        select distinct cns, cpf
        from {{ source("brutos_sheets_staging", "saude_profissionais_cns_cpf_aux") }}
        where cpf is not null and cpf != "" and cpf != "cpf"
    ),

    cns_unicos as (
        select cns, array_agg(distinct cpf) as cpf, count(*) as qtd_cpfs
        from source
        group by cns
        order by qtd_cpfs desc
    )

select *
from cns_unicos
