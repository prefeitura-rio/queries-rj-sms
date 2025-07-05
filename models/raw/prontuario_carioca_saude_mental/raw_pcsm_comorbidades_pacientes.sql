{{
    config(
        schema="brutos_prontuario_carioca_saude_mental",
        alias="comorbidades_pacientes",
        materialized="table",
        tags=["raw", "pcsm", "comorbidades"],
        description="Comorbidades de pacientes atendidos em caps da Prefeitura do Rio de Janeiro."
    )
}}

select
    safe_cast(seqpac as int64) as id_paciente,
    safe_cast(seqcomorb as int64) as id_comorbidade
from
    {{ source('brutos_prontuario_carioca_saude_mental_staging','gh_comorbpacientes') }}