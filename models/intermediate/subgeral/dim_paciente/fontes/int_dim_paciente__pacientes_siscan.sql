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
        
        /*
        safe_cast(paciente_uf as string) as paciente_uf_residencia,
        safe_cast(paciente_municipio as string) as paciente_municipio_residencia,
        safe_cast(paciente_cep as string) as paciente_cep_residencia,
        safe_cast(paciente_bairro as string) as paciente_bairro_residencia,
        safe_cast(paciente_logradouro as string) as paciente_endereco_residencia,
        safe_cast(paciente_endereco_complemento as string) as paciente_complemento_residencia,
        safe_cast(paciente_endereco_numero as string) as paciente_numero_residencia,
        */
        
        safe_cast(paciente_telefone as string) as paciente_telefone
            
    from {{ ref("raw_siscan_web__laudos") }}
    where data_solicitacao >= "2024-01-01"
)

select distinct * from pacientes 
-- paciente_sexo: Feminino, Masculino