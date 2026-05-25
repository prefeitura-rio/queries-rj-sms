with pacientes as (
    select 
        -- id
        cast(null as int) as paciente_cpf,
        safe_cast(paciente_cns as int) as paciente_cns,
        safe_cast(paciente_nome as string) as paciente_nome,
        safe_cast(paciente_data_nasc as date) as paciente_data_nascimento,
        safe_cast(paciente_nome_mae as string) as paciente_nome_mae,

        case 
            when paciente_sexo = "Feminino" then "FEMININO"
            when paciente_sexo = "Masculino" then "MASCULINO"
            else NULL 
        end as paciente_sexo,

        safe_cast(paciente_idade as int64) as paciente_idade,
        
        safe_cast(paciente_telefone as string) as paciente_telefone,

        -- proxy de atualização cadastral: data do laudo mais recente no SISCAN.
        safe_cast(data_solicitacao as timestamp) as data_atualizacao

    from {{ ref("raw_siscan_web__laudos") }}
    where data_solicitacao >= "2024-01-01"
)

select * from pacientes
-- SISCAN não tem cpf; particiona por cns.
qualify row_number() over (
    partition by paciente_cns
    order by data_atualizacao desc
) = 1