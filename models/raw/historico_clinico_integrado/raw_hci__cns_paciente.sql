{{ config(alias="paciente_cns",    materialized="incremental", tags=["hci"]) }}

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
            timestamp(updated_at) as updated_at,
        from source
        {% if is_incremental() %}
        where
            (
                cast(timestamp(updated_at) as date) > '{{seven_days_ago}}'
            )
        {% endif %}
    )
select *
from renamed
