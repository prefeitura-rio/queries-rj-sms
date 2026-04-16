with pacientes as (
    select 
        -- id
        safe_cast(paciente_cpf as int) as paciente_cpf,
        safe_cast(paciente_cns as int) as paciente_cns,
        safe_cast(paciente_nome as string) as paciente_nome,
        safe_cast(paciente_data_nascimento as date) as paciente_data_nascimento,
        safe_cast(paciente_nome_mae as string) as paciente_nome_mae,

        safe_cast(paciente_sexo as string) as paciente_sexo,

        safe_cast(paciente_telefone as string) as paciente_telefone,

        -- proxy de atualização cadastral: data da última solicitação do paciente no SISREG.
        -- não é data de atualização do cadastro, mas indica que o paciente estava ativo no sistema.
        safe_cast(data_solicitacao as timestamp) as data_atualizacao

    from {{ ref("mart_sisreg__solicitacoes") }}
    where data_solicitacao >= TIMESTAMP('2024-01-01 00:00:00')
)

select * from pacientes
-- para cada paciente, mantém apenas o registro da solicitação mais recente.
qualify row_number() over (
    partition by coalesce(safe_cast(paciente_cpf as string), safe_cast(paciente_cns as string))
    order by data_atualizacao desc
) = 1