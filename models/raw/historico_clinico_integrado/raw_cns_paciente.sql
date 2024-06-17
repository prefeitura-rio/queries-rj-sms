{{
    config(
        alias="paciente_cns",
        tags=["hci"]
    )
}}


with
    source as (
        select * from {{ source("historico_clinico_integrado_staging", "paciente_cns") }}
    ),
    renamed as ( 
        select
            safe_cast(id as string) as id,
            safe_cast(patient_id as string) as paciente_id,
            safe_cast(value as string) as cns_valor,
            safe_cast(is_main as bool) as principal,
            safe_cast(fingerprint as string) as fingerprint,
        from source
    )
select *
from renamed
