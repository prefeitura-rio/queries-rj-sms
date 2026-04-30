with pacientes as (
    select
        -- id
        cast(null as int) as paciente_cpf,
        safe_cast(paciente_cns as int) as paciente_cns,
        safe_cast(paciente_nome as string) as paciente_nome,
        safe_cast(paciente_data_nascimento as date) as paciente_data_nascimento,

        safe_cast(paciente_sexo as string) as paciente_sexo,

        -- proxy de atualização cadastral: data da solicitação ambulatorial mais recente no SER.
        safe_cast(data_solicitacao as timestamp) as data_atualizacao

    from {{ ref ("raw_ser_metabase__ambulatorial") }}
    where data_solicitacao >= "2024-01-01"
)

select * from pacientes
-- SER ambulatorial não tem cpf; particiona por cns.
qualify row_number() over (
    partition by paciente_cns
    order by data_atualizacao desc
) = 1
