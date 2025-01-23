{{ 
    config(
        alias="alergias_vitai_padronizacao",
        schema="intermediario_historico_clinico",
        tags=["hci"]
    ) 
}}


with
    source as (
        select *
        from {{ source("intermediario_historico_clinico_staging", "alergias_vitai_padronizacao") }}
    ),
    renamed as (
        select
            safe_cast(alergias_raw as string) as alergias_raw,
            safe_cast(alergias_limpo as string) as alergias_limpo,
            safe_cast(alergias_padronizado as string) as alergias_padronizado
        from source
    )
select *
from renamed
