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
            {{ process_null('cpf') }} as cpf,
            {{ process_null('nome') }} as nome,
            {{ process_null('email') }} as email,
            {{ process_null('telefone') }} as telefone,
            {{ process_null('sub_secretaria__oss') }} as sub_secretaria__oss,
            {{ process_null('area') }} as area,
            {{ process_null('cargo') }} as cargo,
            {{ process_null('status_do_acesso') }} as status_do_acesso,
            {{ process_null('responsavel_pela_renovacao_do_acesso') }} as responsavel_pela_renovacao_do_acesso,
            parse_date(
                '%d/%m/%Y', {{ process_null('ultima_renovacao_do_acesso') }}
            ) as ultima_renovacao_do_acesso,
            split(substr({{ process_null('escopo_bi') }}, 1, length({{ process_null('escopo_bi') }})), ';') as escopo_bi,
            {{ process_null('escopo_ap') }} as escopo_ap,
            coalesce({{ process_null('acesso_tpc') }}, "nao") as acesso_tpc,
            coalesce({{ process_null('acesso_aps') }}, "nao") as acesso_aps,
            coalesce({{ process_null('acesso_upas') }}, "nao") as acesso_upas,
            coalesce({{ process_null('acesso_hospitais') }}, "nao") as acesso_hospitais,
            {{ process_null('acesso_unidade_especifica') }} as acesso_unidade_especifica,
            split(
                substr({{ process_null('acesso_relacao_aps') }}, 1, length({{ process_null('acesso_relacao_aps') }})), ';'
            ) as acesso_relacao_aps,
            split(
                substr(
                    {{ process_null('acesso_relacao_estabelecimentos') }},
                    1,
                    length({{ process_null('acesso_relacao_estabelecimentos') }}) - 1
                ),
                ';'
            ) as acesso_relacao_estabelecimentos,
        from source
    )
select *
from casted
