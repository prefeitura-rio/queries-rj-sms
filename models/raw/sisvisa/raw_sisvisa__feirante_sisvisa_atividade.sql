{{
    config(
        schema="brutos_sisvisa",
        alias="feirante_sisvisa_atividade",
    )
}}

with
    source as (
        select *
        from {{ source("brutos_sisvisa_staging", "AtividadeFeirante") }}
    ),

    dedup as (
        select *
        from source
        qualify
            row_number() over (
                partition by Id
                order by _airbyte_extracted_at desc
            ) = 1
    ),

    renamed as (
        select

            -- ===========================
            -- RELACIONAMENTO FEIRANTE ↔ ATIVIDADE
            -- ===========================
            t.Id             as id,
            t.FeiranteId     as feirante_id,
            t.AtividadeId    as atividade_id

        from dedup t
    )

select *
from renamed