-- Testa se tabela manual esta preenchida corretamente
with
    acessos_manuais as (
        select * from {{ ref("raw_sheets__usuarios_permitidos_hci") }}
    ),
    estabelecimentos as (
        select
            id_cnes,
            area_programatica,
            tipo_sms_simplificado,
            nome_limpo as unidade_nome
        from {{ ref("dim_estabelecimento") }}
    ),

    acessos_cnes_obrigatorio_sem_cnes as (
        select * 
        from acessos_manuais 
        where unidade is null 
        and nivel_de_acesso in ('only_from_same_cnes','only_from_same_ap')
    ),

    acessos_cnes_obrigatorio_sem_cnes_valido as (
        select acessos_manuais.* 
        from acessos_manuais
        left join estabelecimentos
            on acessos_manuais.unidade = estabelecimentos.id_cnes
        where nivel_de_acesso in ('only_from_same_cnes','only_from_same_ap') 
        and estabelecimentos.area_programatica is null
    ),
    acessos_sem_cpf_valido as (
        select * 
        from acessos_manuais 
        where {{ validate_cpf('cpf') }} is false
    )

select * from acessos_cnes_obrigatorio_sem_cnes
union all
select * from acessos_cnes_obrigatorio_sem_cnes_valido
union all
select * from acessos_sem_cpf_valido
