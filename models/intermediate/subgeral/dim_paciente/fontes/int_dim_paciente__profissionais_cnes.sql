with profissionais as (
    select distinct
        -- id
        safe_cast(cpf as int) as paciente_cpf,
        safe_cast(cns as int) as paciente_cns,
        safe_cast(nome as string) as paciente_nome
    from {{ ref("dim_profissional_sus_rio_historico") }}
    where ano_competencia >= 2024
)

select * from profissionais