{{
    config(
        schema="brutos_sheets",
        alias="profissionais_cns_cpf_aux",
        tags=["daily", "subgeral", "cnes_subgeral", "monitora_reg"],
    )
}}
-- TODO: conferir tags acima

with
    source as (
        select distinct cns, cpf
        from {{ source("brutos_sheets_staging", "profissionais_cns_cpf_aux") }}
        where cpf is not null and cpf != "" and cpf != "cpf"
    ),

    tratados as (
        select
            lpad(safe_cast(cns as string), 15, "0") as cns,
            lpad(safe_cast(cpf as string), 11, "0") as cpf,
        from source
    ),

    cns_unicos as (
        select cns, array_agg(distinct cpf) as cpf, count(*) as qtd_cpfs
        from tratados
        group by cns
        order by qtd_cpfs desc
    )

select *
from cns_unicos
