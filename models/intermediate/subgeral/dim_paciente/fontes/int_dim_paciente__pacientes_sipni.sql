with pacientes as (
    select  
        -- id
        safe_cast(paciente_cpf as int) as paciente_cpf,
        safe_cast(paciente_cns as int) as paciente_cns,
        safe_cast(paciente_nome as string) as paciente_nome, 
        safe_cast(paciente_nascimento_data as date) as paciente_data_nascimento,
        safe_cast(paciente_nome_mae as string) as paciente_nome_mae,
        
        case
            when paciente_sexo = 'M' then 'Masculino'
            when paciente_sexo = 'F' then 'Feminino'
            else NULL
        end as paciente_sexo,

        -- proxy de atualização cadastral: data da vacinação mais recente no SIPNI.
        safe_cast(vacina_aplicacao_data as timestamp) as data_atualizacao,

    from {{ ref("raw_sipni__vacinacao") }}
    where vacina_aplicacao_data >= "2024-01-01"
)

select * from pacientes
qualify row_number() over (
    partition by coalesce(safe_cast(paciente_cpf as string), safe_cast(paciente_cns as string))
    order by data_atualizacao desc
) = 1