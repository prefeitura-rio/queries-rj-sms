{{
    config(
        schema="brutos_sheets",
        alias="gerenciamento_acesso_looker_farmacia",
    )
}}
with
    source as (
        select *
        from
            {{
                source(
                    "brutos_sheets_staging", "gerenciamento_acesso_looker_farmacia"
                )
            }}
    ),
    casted as (
        select
            cpf,
            nome,
            email,
            telefone,
            sub_secretaria__oss,
            area,
            cargo,
            status_do_acesso,
            responsavel_pela_renovacao_do_acesso,
            parse_date(
                '%d/%m/%Y', ultima_renovacao_do_acesso
            ) as ultima_renovacao_do_acesso,
            escopo_bi,
            escopo_ap,
            coalesce(acesso_tpc, "nao") as acesso_tpc,
            coalesce(acesso_aps, "nao") as acesso_aps,
            coalesce(acesso_upas, "nao") as acesso_upas,
            coalesce(acesso_hospitais, "nao") as acesso_hospitais,
            acesso_unidade_especifica
        from source
    )
select *
from casted
