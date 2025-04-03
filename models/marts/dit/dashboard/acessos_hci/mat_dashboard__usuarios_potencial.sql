{{
    config(
        alias="hci_usuarios_potencial",
        tags="dashboard_tables"
    )
}}
with profissional as (
    select * 
    from {{ref('dim_vinculo_profissional_saude_estabelecimento')}}
),
estabelecimento as (
    select * 
    from {{ref('dim_estabelecimento')}}
)
select 
    profissional.id_profissional_sus,
    profissional.profissional_cns,
    profissional.cbo,
    profissional.cbo_nome,
    profissional.cbo_agrupador,
    profissional.cbo_familia_nome,
    profissional.id_cnes,
    estabelecimento.area_programatica,
from profissional 
left join estabelecimento 
on profissional.id_cnes = estabelecimento.id_cnes