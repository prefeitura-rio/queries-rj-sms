{{ config(alias="paciente_contato", tags=["hci"]) }}


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
        from source
    )
select *
from renamed
