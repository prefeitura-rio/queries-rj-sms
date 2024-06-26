{{ config(alias="paciente_cns", tags=["hci"]) }}


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
        from source
    )
select *
from renamed
