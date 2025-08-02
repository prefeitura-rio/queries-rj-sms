{{
    config(
        alias="email",
        schema="projeto_cdi",
        materialized="table",
    )
}}

with diario_municipal as (
    select distinct
        data_publicacao,
        'Diário Oficial do Município' as fonte,
        pasta,
        content_email,
        voto,
        link
    from {{ ref('int_cdi__diario_oficial_rj_filtrado') }}
),
diario_uniao as (
    select distinct
        data_publicacao,
        'Diário Oficial da União' as fonte,
        cast(null as string) as pasta,
        content_email,
        cast(null as string) as voto,
        link 
    from {{ ref('int_cdi__diario_oficial_uniao_filtrado')}}
)

select * from diario_municipal
union all
select * from diario_uniao