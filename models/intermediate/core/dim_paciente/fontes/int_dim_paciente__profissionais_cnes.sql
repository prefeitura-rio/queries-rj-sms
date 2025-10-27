with profissionais as (
    select distinct
        -- id
        cast(cpf as string) as paciente_cpf,
        cast(cns as string) as paciente_cns,
        cast(nome as string) as paciente_nome
    from {{ ref("dim_profissional_sus_rio_historico") }}
    where ano_competencia >= 2024
)

select * from profissionais