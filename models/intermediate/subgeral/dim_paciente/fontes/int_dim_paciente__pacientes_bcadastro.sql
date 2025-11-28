with pacientes as (
    select 
        safe_cast(cpf_particao as int) as paciente_cpf,
        
        nome as paciente_nome,
        mae_nome as paciente_nome_mae,
        nascimento_data as paciente_data_nascimento,

        nome_social as paciente_nome_social,
        upper(sexo) as paciente_sexo,

        /*
        endereco.complemento as paciente_complemento_residencia,
        endereco.numero as paciente_numero_residencia,
        endereco.cep as paciente_cep_residencia,
        endereco.logradouro as paciente_endereco_residencia,
        endereco.tipo_logradouro as paciente_tp_logradouro_residencia,
        endereco.bairro as paciente_bairro_residencia,
        endereco.municipio as paciente_municipio_residencia,
        endereco.uf as paciente_uf_residencia,
        */

        concat(
            coalesce(contato.telefone.ddi, ''),
            coalesce(contato.telefone.ddd, ''),
            coalesce(contato.telefone.numero, '')
        ) as paciente_telefone,
        --contato.email as paciente_email,      
    
        safe_cast(obito_ano as int) as paciente_obito_ano

    from {{ source("brutos_bcadastro_sms", "cpf") }}
    where
        nascimento_local.uf = "rj"
        or endereco.uf = "rj"
)

select * from pacientes 
-- paciente_sexo: feminino, masculino
