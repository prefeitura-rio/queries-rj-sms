{{
    config(
        schema="brutos_prontuario_carioca_saude_mental",
        alias="drogas_pacientes",
        materialized="table",
        tags=["raw", "pcsm", "drogas"],
        description="Drogas usadas por pacientes atendidos por caps da Prefeitura do Rio de Janeiro."
    )
}}

select
    safe_cast(seqpac as int64) as id_paciente,
    safe_cast(seqdroga as int64) as id_droga,
    _airbyte_extracted_at as loaded_at,
    current_timestamp() as transformed_at
from
    {{ source('brutos_prontuario_carioca_saude_mental_staging','gh_drogaspacientes') }}