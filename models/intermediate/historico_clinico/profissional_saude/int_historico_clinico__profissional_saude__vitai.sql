with profissionais_std as (
    select  gid, cns, cpf, INITCAP(nome) as nome
    from {{ ref('raw_prontuario_vitai__profissional')}}
)
select * from profissionais_std