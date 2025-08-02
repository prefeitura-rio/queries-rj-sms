{{
    config(
        schema="brutos_prontuario_carioca_saude_mental",
        alias="drogas",
        materialized="table",
        tags=["raw", "pcsm", "drogas"],
        description="Drogas possíveis para pacientes."
    )
}}

select
    safe_cast(seqdroga as int64) as id_droga,
    safe_cast(descdroga as string) as descricao_droga,
    safe_cast(dscdrogadet as string) as descricao_detalhada_droga,
    _airbyte_extracted_at as loaded_at,
    current_timestamp() as transformed_at
from {{ source('brutos_prontuario_carioca_saude_mental_staging', 'gh_drogas') }}