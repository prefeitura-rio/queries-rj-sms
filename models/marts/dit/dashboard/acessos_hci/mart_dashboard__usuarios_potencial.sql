{{
    config(
        schema="dashboard_historico_clinico",
        alias="hci_usuarios_potencial",
        tags="dashboard_tables"
    )
}}
with profissional as (
    select * 
    from {{ref('mart_historico_clinico_app__acessos')}}
),
estabelecimento as (
    select * 
    from {{ref('dim_estabelecimento')}}
)

select 
    id_cnes,
    estabelecimento.area_programatica,
    estabelecimento.nome_limpo as unidade_nome,
    profissional.funcao_grupo,
    profissional.cpf as cpf_usuario,
from profissional 
left join estabelecimento 
on profissional.unidade_cnes = estabelecimento.id_cnes