with pacientes as (
  select distinct
    -- id
    safe_cast(usuariocpf as int) as paciente_cpf,
    safe_cast(usuariocns as int) as paciente_cns,
    safe_cast(usuarionome as string) as paciente_nome

    -- nao estao sendo utilizados:
    /*
    unidaderefcnes,
    unidaderefnome,
    unidaderefcap
    */
    
  from {{ ref("raw_centralderegulacao_mysql__fibromialgia_relatorio") }}
  where date(solicitacaodatahora) >= date '2024-01-01'
)

select * from pacientes