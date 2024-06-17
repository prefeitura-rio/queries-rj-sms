{{
    config(
        alias="paciente_endereco",
        tags=["hci"]
    )
}}


with
    source as (
        select * from {{ source("historico_clinico_integrado_staging", "paciente_endereco") }}
    ),
    renamed as ( 
        select
            safe_cast(id as string) as id,
            safe_cast(patient_id as string) as paciente_id,
            safe_cast(use as string) as uso,
            safe_cast(type as bool) as tipo,
            safe_cast(line as string) as logradouro,
            safe_cast(postal_code as string) as cep,
            safe_cast(city_id as string) as cidade_id,
            safe_cast(fingerprint as string) as fingerprint,
            safe_cast(period_start as date) as inicio_periodo,
            safe_cast(period_end as date) as fim_periodo,
        from source
    )
select *
from renamed
