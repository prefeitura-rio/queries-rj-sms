with profissionais as (
    select
        -- id
        safe_cast(cpf as int) as paciente_cpf,
        safe_cast(cns as int) as paciente_cns,
        safe_cast(nome as string) as paciente_nome,

        -- proxy de atualização cadastral: última competência (ano-mês) em que o profissional
        -- aparece no CNES. granularidade mensal; usa dia 1 como representação da competência.
        timestamp(
            date(
                safe_cast(ano_competencia as int64),
                safe_cast(mes_competencia as int64),
                1
            )
        ) as data_atualizacao

    from {{ ref("dim_profissional_sus_rio_historico") }}
    where ano_competencia >= 2024
)

select * from profissionais
qualify row_number() over (
    partition by coalesce(safe_cast(paciente_cpf as string), safe_cast(paciente_cns as string))
    order by data_atualizacao desc
) = 1