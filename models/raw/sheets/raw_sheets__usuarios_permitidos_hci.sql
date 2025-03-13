{{
    config(
        schema="brutos_sheets",
        alias="usuarios_permitidos_hci",
    )
}}

with
    source as (
        select *
        from {{ source("brutos_sheets_staging", "usuarios_permitidos_hci") }}
    ),

    tratados as (
        select
            * except(cpf, nome_unidade, nivel_de_acesso),
            lpad(safe_cast(cpf as string), 11, "0") as cpf,
            trim(nome_unidade) as unidade_nome,
            {{ process_null('nivel_de_acesso') }} as nivel_de_acesso
        from source
    ),

    distintos as (
        select distinct *
        from tratados
    )

select
    *
from distintos
where {{ validate_cpf('cpf') }}
