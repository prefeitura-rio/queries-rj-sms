{{
    config(
        schema="gerenciamento__acessos",
        alias="looker_farmacia_digital",
    )
}}

with
    acessos as (
        select * from {{ ref("raw_sheets__gerenciamento_acesso_looker_farmacia") }}
    ),

    estabelecimentos as (
        select id_cnes, area_programatica, tipo_sms_agrupado, nome_limpo
        from {{ ref("dim_estabelecimento") }}
    ),

    relacao_acesso_estabelecimento as (
        select
            a.email,
            a.cpf,
            a.nome,
            a.sub_secretaria__oss,
            a.area,
            a.cargo,
            a.status_do_acesso,
            a.escopo_bi,
            a.acesso_relacao_aps,
            a.acesso_relacao_estabelecimentos,
            e.id_cnes,
            e.area_programatica,
            e.tipo_sms_agrupado,
            e.nome_limpo,
        from acessos as a
        cross join estabelecimentos as e
        where
            e.area_programatica in unnest(a.acesso_relacao_aps)
            and (
                e.id_cnes in unnest(a.acesso_relacao_estabelecimentos)
                or e.tipo_sms_agrupado in unnest(a.acesso_relacao_estabelecimentos)
            )
    ),

    relacao_acesso_tpc as (
        select
            email,
            cpf,
            nome,
            sub_secretaria__oss,
            area,
            cargo,
            status_do_acesso,
            escopo_bi,
            acesso_relacao_aps,
            acesso_relacao_estabelecimentos,
            'tpc' as id_cnes,
            'TPC' as area_programatica,
            'TPC' as tipo_sms_agrupado,
            'TPC' as nome_limpo,
        from acessos
        where 'TPC' in unnest(acesso_relacao_estabelecimentos)
    ),

    relacao_union as (
        select * from relacao_acesso_tpc
        union all
        select * from relacao_acesso_estabelecimento
    )
select *
from relacao_union
where status_do_acesso = "ativo"
order by email, area_programatica, tipo_sms_agrupado, nome_limpo
