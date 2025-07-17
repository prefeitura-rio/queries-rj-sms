{{
    config(
        schema="brutos_prontuario_carioca_saude_mental",
        alias="servico_residencial_terapeutico_pacientes",
        materialized="table",
        tags=["raw", "pcsm", "srt"],
        description="Pacientes cadastrados em SRT (Serviço Residencial Terapêutico)."
    )
}}

select
    safe_cast(seqpac as int64) as id_paciente,
    safe_cast(seqsrt as int64) as id_servico_residencial,
    safe_cast(dt_cadastro as date) as data_cadastro,
    _airbyte_extracted_at as loaded_at,
    current_timestamp() as transformed_at
from
    {{ source('brutos_prontuario_carioca_saude_mental_staging','gh_srtpacientes') }}