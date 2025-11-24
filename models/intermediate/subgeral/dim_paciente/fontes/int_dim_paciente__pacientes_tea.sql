with pacientes as (
    select distinct 
        -- id
        cast(usuariocpf as string) as paciente_cpf,
        cast(usuariocns as string) as paciente_cns,
        cast(usuarionome as string) as paciente_nome
    from {{ ref("raw_centralderegulacao_mysql__tea_relatorio") }}
    where date(solicitacaodatahora) >= date '2024-01-01'
)

select * from pacientes