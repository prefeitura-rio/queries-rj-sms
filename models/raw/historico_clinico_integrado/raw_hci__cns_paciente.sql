{{
    config(
        alias="paciente_cns",
        materialized="incremental",
        unique_key="id",
        tags=["hci"],
    )
}}

{% set seven_days_ago = (
    modules.datetime.date.today() - modules.datetime.timedelta(days=7)
).isoformat() %}

with
    source as (
        select *
        from {{ source("brutos_historico_clinico_integrado_staging", "paciente_cns") }}
    ),
    renamed as (
        select
            safe_cast(id as string) as id,
            safe_cast(patient_id as string) as id_paciente,
            safe_cast(value as string) as cns_valor,
            safe_cast(is_main as bool) as principal,
            timestamp(created_at) as created_at,
            (
                row_number() over (partition by id order by created_at desc)
                = 1
            ) as is_latest
        from source
        {% if is_incremental() %}
            where (cast(timestamp(created_at) as date) > '{{seven_days_ago}}')
        {% endif %}
    )
select *
from renamed
where is_latest = true
