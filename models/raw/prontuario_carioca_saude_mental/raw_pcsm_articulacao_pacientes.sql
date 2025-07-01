{{
    config(
        schema="brutos_prontuario_carioca_saude_mental",
        alias="articulacao_pacientes",
        materialized="table",
        tags=["raw", "pcsm", "articulacao", "pacientes"],
        description="Pacientes que participaram de articulações."
    )
}}

select
    safe_cast(seqarticula as int64) as id_articulacao,
    safe_cast(seqpac as int64) as id_paciente,
    safe_cast(indevolpac as string) as paciente_evoluido
from
    {{ source('brutos_prontuario_carioca_saude_mental_staging','gh_articula_paciente') }}