with pacientes as (
    select 
        safe_cast(cpf_particao as int) as paciente_cpf,
        
        nome as paciente_nome,
        mae_nome as paciente_nome_mae,
        nascimento_data as paciente_data_nascimento,

        nome_social as paciente_nome_social,
        upper(sexo) as paciente_sexo,

        concat(
            coalesce(contato.telefone.ddi, ''),
            coalesce(contato.telefone.ddd, ''),
            coalesce(contato.telefone.numero, '')
        ) as paciente_telefone,
    
        safe_cast(obito_ano as int) as paciente_obito_ano,

        -- bcadastro (Receita Federal) é um snapshot sem granularidade por paciente de atualização.
        -- mantemos null no desempate, a prioridade do bcadastro vem da ordem fixa de sistemas.
        cast(null as timestamp) as data_atualizacao

    from {{ source("brutos_bcadastro_sms", "cpf") }}
    where
        nascimento_local.municipio = "Rio de Janeiro"
        or endereco.municipio = "Rio de Janeiro"
)

select * from pacientes