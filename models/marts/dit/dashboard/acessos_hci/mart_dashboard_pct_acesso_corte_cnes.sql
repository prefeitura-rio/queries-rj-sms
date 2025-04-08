{{
    config(
        schema="dashboard_historico_clinico",
        alias="hci_usuarios_pct_acesso_cnes",
        tags="dashboard_tables"
    )
}}
with usuario_acessando as (
    select unidade_nome, cnes_lotacao,area_programatica, count(distinct cpf_usuario) as total_usuarios_acessando
    from {{ref('mart_dashboard__usuario')}}
    group by 1,2,3
),
usuarios_com_acesso as (
    select unidade_nome, count(distinct cpf_usuario) as total_usuarios_acesso
    from {{ref('mart_dashboard__usuarios_potencial')}}
    group by 1
)
select usuario_acessando.unidade_nome,cnes_lotacao,area_programatica, total_usuarios_acessando/total_usuarios_acesso as percentual_acesso
from usuario_acessando
left join usuarios_com_acesso
on usuario_acessando.unidade_nome = usuarios_com_acesso.unidade_nome