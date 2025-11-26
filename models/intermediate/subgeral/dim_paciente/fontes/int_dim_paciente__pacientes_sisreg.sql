with pacientes as (
    select distinct
        -- id
        safe_cast(paciente_cpf as int) as paciente_cpf,
        safe_cast(paciente_cns as int) as paciente_cns,
        safe_cast(paciente_nome as string) as paciente_nome,
        safe_cast(paciente_data_nascimento as date) as paciente_data_nascimento,
        safe_cast(paciente_nome_mae as string) as paciente_nome_mae,

        safe_cast(paciente_sexo as string) as paciente_sexo,
        safe_cast(paciente_uf_nascimento as string) as paciente_uf_nascimento,
        safe_cast(paciente_municipio_nascimento as string) as paciente_municipio_nascimento,

        safe_cast(paciente_complemento_residencia as string) as paciente_complemento_residencia,
        safe_cast(paciente_numero_residencia as string) as paciente_numero_residencia,
        safe_cast(paciente_cep_residencia as string) as paciente_cep_residencia,
        safe_cast(paciente_endereco_residencia as string) as paciente_endereco_residencia,
        safe_cast(paciente_tp_logradouro_residencia as string) as paciente_tp_logradouro_residencia,
        safe_cast(paciente_bairro_residencia as string) as paciente_bairro_residencia,
        safe_cast(paciente_municipio_residencia as string) as paciente_municipio_residencia,
        safe_cast(paciente_uf_residencia as string) as paciente_uf_residencia,

        safe_cast(paciente_telefone as string) as paciente_telefone
    
    from {{ ref("mart_sisreg__solicitacoes") }}
    where date(data_solicitacao) >= date '2024-01-01'
)

select * from pacientes
-- paciente_sexo: MASCULINO, FEMININO