{{
    config(
        alias="telefone",
        materialized="table",
    )
}}

with

    telefones_vitacare as (
        select 
            cpf,
            upper(nome) as nome,
            upper(nome_mae) as mae_nome,
            data_nascimento as nascimento_data,

            cast(null as string) as ddd,
            telefone,

            'vitacare' as origem,

            updated_at
        from {{ ref("raw_prontuario_vitacare_historico__cadastro") }}
        where telefone is not null
    ),

    telefones_vitai as (
        select 
            cpf,
            upper(nome) as nome,
            upper(nome_mae) as mae_nome,
            data_nascimento as nascimento_data,

            cast(null as string) as ddd,
            telefone,

            'vitai' as origem,

            updated_at
        from {{ ref("raw_prontuario_vitai__paciente") }}
        where telefone is not null
    ),

    telefone_smsrio as (
        select 
            cpf,
            upper(nome) as nome,
            upper(nome_mae) as mae_nome,
            data_nascimento as nascimento_data,

            cast(null as string) as ddd,
            telefone,

            'plataforma-smsrio' as origem,
            
            updated_at
        from {{ ref("raw_plataforma_smsrio__paciente_cadastro") }} cadastro
        where telefone is not null
    ),

    telefone_smsrio_outros as (
        select 
            cpf,
            upper(nome) as nome,
            upper(nome_mae) as mae_nome,
            data_nascimento as nascimento_data,

            cast(null as string) as ddd,
            telefone_outros.telefone,

            'plataforma-smsrio' as origem,

            telefone_outros.updated_at
        from {{ ref("raw_plataforma_smsrio__paciente_cadastro") }} cadastro
            inner join {{ ref("raw_plataforma_smsrio__paciente_telefones") }} telefone_outros on cadastro.cns = telefone_outros.cns
    ),

    juncao as (
        select * from telefones_vitacare
        union all
        select * from telefones_vitai
        union all
        select * from telefone_smsrio
        union all
        select * from telefone_smsrio_outros
    )


select *
from juncao