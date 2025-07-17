{{
    config(
        schema="brutos_prontuario_carioca_saude_mental",
        alias="matriciamento_pacientes",
        materialized="table",
        tags=["raw", "pcsm", "matriciamento", "pacientes"],
        description="Pacientes participantes de matriciamento."
    )
}}

select
    safe_cast(seqmatric as int64) as id_matriciamento,
    safe_cast(seqpac as int64) as id_paciente,
    safe_cast(indevolpac as string) as paciente_evoluido,
    case trim(safe_cast(indevolpac as string))
        when 'S' then 'Sim'
        when 'N' then 'Não'
        when '' then 'Não'
        when null then 'Não'
        else 'Não classificado'
    end as descricao_paciente_evoluido,
    _airbyte_extracted_at as loaded_at,
    current_timestamp() as transformed_at
from
    {{ source('brutos_prontuario_carioca_saude_mental_staging','gh_matric_paciente') }}