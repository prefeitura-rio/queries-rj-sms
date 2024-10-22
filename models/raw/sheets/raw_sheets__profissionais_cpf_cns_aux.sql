{{
    config(
        schema="brutos_sheets",
        alias="profissionais_cpf_cns_aux",
    )
}}

with
source as (
      select distinct cns_consulta,
        cns_definitivo,
        cns_provisorio,
        nome_completo,
        cpf

from {{ source('brutos_sheets-dev', 'profissionais_cpf_cns_aux') }}
where cpf is not null and cpf != "" and cpf != "cpf"
),

melted as (
    select
        cns_consulta as cns,
        cpf
    from
        source
    where
        cns_consulta is not null

    union distinct

    select
        cns_definitivo as cns,
        cpf
    from
        source
    where
        cns_definitivo is not null

    union distinct

    select
        cns_provisorio as cns,
        cpf
    from
        source
    where
        cns_provisorio is not null
)

select distinct cpf, cns from melted