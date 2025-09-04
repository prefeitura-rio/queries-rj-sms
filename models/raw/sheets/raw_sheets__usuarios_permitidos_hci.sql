{{
    config(
        schema="brutos_sheets",
        alias="usuarios_permitidos_hci",
        tags=["daily", "hci"],
    )
}}

with
    source as (
        select *
        from {{ source("brutos_sheets_staging", "usuarios_permitidos_hci") }}
    ),

    tratados as (
        select
            * except(cpf, unidade, nivel_de_acesso),
            lpad(safe_cast(cpf as string), 11, "0") as cpf,
            lpad(trim(unidade),7,"0") as unidade,
            {{ process_null('nivel_de_acesso') }} as nivel_de_acesso
        from source
    ),

    distintos as (
        select *
        from tratados
        qualify row_number() over (partition by cpf order by nivel_de_acesso) = 1
    )

select
    *
from distintos
where {{ validate_cpf('cpf') }}
