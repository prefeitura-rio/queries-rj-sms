{{
    config(
        schema="brutos_prontuario_carioca_saude_mental",
        alias="comorbidades",
        materialized="table",
        tags=["raw", "pcsm", "comorbidades"],
        description="Comorbidades possíveis para pacientes."
    )
}}

select
    safe_cast(seqcomorb as int64) as id_comorbidade,
    safe_cast(desccomorb as string) as descricao_comorbidade,
    safe_cast(codcid as string) as codigo_doenca,
    _airbyte_extracted_at as loaded_at,
    current_timestamp() as transformed_at
from {{ source('brutos_prontuario_carioca_saude_mental_staging', 'gh_comorbidades') }}