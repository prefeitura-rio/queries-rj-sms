{{ 
    config(
        alias="paciente_endereco", 
        tags=["hci"],
        materialized="incremental",
        ) }}

{% set seven_days_ago = (
    modules.datetime.date.today() - modules.datetime.timedelta(days=7)
).isoformat() %}

with
    source as (
        select *
        from
            {{
                source(
                    "brutos_historico_clinico_integrado_staging", "paciente_endereco"
                )
            }}
    ),
    renamed as (
        select
            safe_cast(id as string) as id,
            safe_cast(patient_id as string) as id_paciente,
            safe_cast(use as string) as uso,
            safe_cast(type as bool) as tipo,
            safe_cast(line as string) as logradouro,
            safe_cast(postal_code as string) as cep,
            safe_cast(city_id as string) as id_cidade,
            safe_cast(period_start as date) as periodo_inicio,
            safe_cast(period_end as date) as periodo_fim,
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
