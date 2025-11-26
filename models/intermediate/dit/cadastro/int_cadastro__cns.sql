{{
    config(
        alias="cns",
        materialized="table",
    )
}}

with

    cns_vitacare as (
        select 
            cpf,
            upper(nome) as nome,
            upper(nome_mae) as mae_nome,
            data_nascimento as nascimento_data,

            cns,

            updated_at as updated_at
        from {{ ref("raw_prontuario_vitacare_historico__cadastro") }}
    ),

    cns_vitai as (
        select 
            cpf,
            upper(nome) as nome,
            upper(nome_mae) as mae_nome,
            data_nascimento as nascimento_data,

            cns,

            updated_at
        from {{ ref("raw_prontuario_vitai__paciente") }}
    ),

    cns_smsrio as (
        select 
            cpf,
            upper(nome) as nome,
            upper(nome_mae) as mae_nome,
            data_nascimento as nascimento_data,

            cns,
            
            updated_at
        from {{ ref("raw_plataforma_smsrio__paciente_cadastro") }} cadastro
        where cns is not null
    ),

    cns_smsrio_outros as (
        select 
            cpf,
            upper(nome) as nome,
            upper(nome_mae) as mae_nome,
            data_nascimento as nascimento_data,

            cns_provisorio as cns,

            cns_outros.updated_at
        from {{ ref("raw_plataforma_smsrio__paciente_cadastro") }} cadastro
            inner join {{ ref("raw_plataforma_smsrio__paciente_cns") }} cns_outros on cadastro.cns = cns_outros.cns
    ),

    juncao as (
        select * from cns_vitacare
        union all
        select * from cns_vitai
        union all
        select * from cns_smsrio
        union all
        select * from cns_smsrio_outros
    )

select * from juncao
