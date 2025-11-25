{{
    config(
        schema="brutos_sisvisa",
        alias="grupo_atividade_classificada",
    )
}}

with
    source as (
        select *
        from {{ source("brutos_sisvisa_staging", "GrupoAtividadeClassificada") }}
    ),

    dedup as (
        select *
        from source
        qualify
            row_number() over (
                partition by AtividadeId, GrupoAtividadeId
                order by _airbyte_extracted_at desc
            ) = 1
    ),

    renamed as (
        select
            t.AtividadeId          as atividade_id,
            t.GrupoAtividadeId     as grupo_atividade_id

        from dedup t
    )

select *
from renamed