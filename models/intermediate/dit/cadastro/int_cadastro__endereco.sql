{{
    config(
        alias="endereco",
        materialized="table",
    )
}}

with

    enderecos_vitacare as (
        select 
            cpf,
            upper(nome) as nome,
            upper(nome_mae) as mae_nome,
            data_nascimento as nascimento_data,

            cep as cep,
            tipo_logradouro as tipo_logradouro,
            logradouro as logradouro,
            cast(null as string) as numero,
            cast(null as string) as complemento,
            bairro as bairro,
            municipio_residencia as municipio,
            estado_residencia as uf,

            'vitacare' as origem,

            updated_at as updated_at
        from {{ ref("raw_prontuario_vitacare_historico__cadastro") }}
        where (
            logradouro is not null AND
            bairro is not null
        )
    ),

    enderecos_vitai as (
        select 
            cpf,
            upper(nome) as nome,
            upper(nome_mae) as mae_nome,
            data_nascimento as nascimento_data,

            cast(null as string) as cep,
            tipo_logradouro,
            nome_logradouro as logradouro,
            numero,
            complemento,
            bairro,
            municipio,
            uf,

            'vitai' as origem,

            updated_at
        from {{ ref("raw_prontuario_vitai__paciente") }}
        where (
            nome_logradouro is not null AND
            bairro is not null
        )
    ),

    juncao as (
        select * from enderecos_vitacare
        union all
        select * from enderecos_vitai
    )


select *
from juncao

