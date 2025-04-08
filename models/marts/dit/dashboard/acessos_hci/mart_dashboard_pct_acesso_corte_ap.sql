{{
    config(
        schema="dashboard_historico_clinico",
        alias="hci_usuarios_pct_acesso_ap",
        tags="dashboard_tables"
    )
}}
with usuario_acessando as (
    select area_programatica, count(distinct cpf_usuario) as total_usuarios_acessando
    from {{ref('mart_dashboard__usuario')}}
    group by 1
),
usuarios_com_acesso as (
    select area_programatica, count(distinct cpf_usuario) as total_usuarios_acesso
    from {{ref('mart_dashboard__usuarios_potencial')}}
    group by 1
)
select usuario_acessando.area_programatica, total_usuarios_acessando/total_usuarios_acesso as percentual_acesso
from usuario_acessando
left join usuarios_com_acesso
on usuario_acessando.area_programatica = usuarios_com_acesso.area_programatica