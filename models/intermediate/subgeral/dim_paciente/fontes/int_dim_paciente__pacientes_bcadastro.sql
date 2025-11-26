with pacientes as (
    select 
        safe_cast(cpf_particao as int) as paciente_cpf,
        
        nome as paciente_nome,
        mae_nome as paciente_nome_mae,
        nascimento_data as paciente_data_nascimento,

        nome_social as paciente_nome_social,
        upper(sexo) as paciente_sexo,
        
        /*
        safe_cast(obito_ano as int) as paciente_obito_ano,

        endereco.cep,
        endereco.pais,
        endereco.uf,
        endereco.municipio,
        endereco.bairro,
        endereco.tipo_logradouro,
        endereco.logradouro,
        endereco.numero,
        endereco.complemento,

        concat(
            coalesce(contato.telefone.ddi, ''),
            coalesce(contato.telefone.ddd, ''),
            coalesce(contato.telefone.numero, '')
        ) as paciente_telefone_contato
        
        contato.email,
        */
        
    from {{ source("brutos_bcadastro_sms", "cpf") }}
)

select * from pacientes 
-- paciente_sexo: feminino, masculino
