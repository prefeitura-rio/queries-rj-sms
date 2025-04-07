{{
    config(
        schema="dashboard_historico_clinico",
        alias="hci_usuarios_potencial",
        tags="dashboard_tables"
    )
}}
with profissional as (
    select * 
    from {{ref('int_acessos__automatico')}}
    union all 
    select *
    from {{ref('int_acessos__manual')}}
),
estabelecimento as (
    select * 
    from {{ref('dim_estabelecimento')}}
)
select 
    id_cnes,
    estabelecimento.area_programatica,
    profissional.*,
    estabelecimento.area_programatica,
from profissional 
left join estabelecimento 
on profissional.unidade_cnes = estabelecimento.id_cnes