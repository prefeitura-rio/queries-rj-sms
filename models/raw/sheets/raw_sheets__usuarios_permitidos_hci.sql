{{
    config(
        schema="brutos_sheets",
        alias="usuarios_permitidos_hci",
        partition_by={
            "field": "cpf_particao",
            "data_type": "int64",
            "range": {"start": 0, "end": 100000000000, "interval": 34722222},
        },
    )
}}

with
    source as (
        select *
        from {{ source("brutos_sheets_staging", "usuarios_permitidos_hci") }}
    ),

    tratados as (
        select
            safe_cast(cpf as int64) as cpf_particao,
            lpad(safe_cast(cpf as string), 11, "0") as cpf,
            cnes,
            nivel_de_acesso as nivel_acesso
        from source
    ),

    distintos as (
        select distinct *
        from tratados
    )

select *
from distintos
where {{ validate_cpf('cpf') }}
