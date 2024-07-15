{{
    config(
        alias="paciente_contato",
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
        from
            {{
                source(
                    "brutos_historico_clinico_integrado_staging", "paciente_contato"
                )
            }}
    ),
    renamed as (
        select
            safe_cast(id as string) as id,
            safe_cast(patient_id as string) as id_paciente,
            safe_cast(use as string) as uso,
            safe_cast(system as string) as tipo,
            safe_cast(value as string) as valor,
            safe_cast(rank as int) as rank,
            safe_cast(period_start as date) as periodo_inicio,
            safe_cast(period_end as date) as periodo_fim,
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
