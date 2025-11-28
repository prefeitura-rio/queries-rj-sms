with pacientes as (
    select distinct
        -- id
        cast(null as int) as paciente_cpf,
        safe_cast(paciente_cns as int) as paciente_cns,
        safe_cast(paciente_nome as string) as paciente_nome,
        safe_cast(paciente_data_nascimento as date) as paciente_data_nascimento,

        safe_cast(paciente_sexo as string) as paciente_sexo,
        --safe_cast(paciente_municipio as string) as paciente_municipio_residencia
        
    from {{ ref ("raw_ser_metabase__ambulatorial") }}
    where data_solicitacao >= TIMESTAMP('2024-01-01 00:00:00')
)

select * from pacientes
-- paciente_sexo: MASCULINO, FEMININO, NULL