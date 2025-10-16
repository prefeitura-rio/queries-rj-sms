{{
    config(
        schema="brutos_sheets",
        alias="gerenciamento_acesso_looker_farmacia",
        tags=["daily"],
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
            split(substr(escopo_bi, 1, length(escopo_bi)), ';') as escopo_bi,
            escopo_ap,
            coalesce(acesso_tpc, "nao") as acesso_tpc,
            coalesce(acesso_aps, "nao") as acesso_aps,
            coalesce(acesso_upas, "nao") as acesso_upas,
            coalesce(acesso_hospitais, "nao") as acesso_hospitais,
            acesso_unidade_especifica,
            split(
                substr(acesso_relacao_aps, 1, length(acesso_relacao_aps)), ';'
            ) as acesso_relacao_aps,
            split(
                substr(
                    acesso_relacao_estabelecimentos,
                    1,
                    length({{ process_null('acesso_relacao_estabelecimentos') }}) - 1
                ),
                ';'
            ) as acesso_relacao_estabelecimentos,
        from source
    )
select *
from casted
