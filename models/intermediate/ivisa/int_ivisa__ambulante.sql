{{
    config(
        schema="intermediario_empreendimentos_cariocas",
        alias="int_ambulante",
        materialized="table",
    )
}}

with 

ambulantes_no_sisvisa as (
    select
        id,

        cpf,
        cnpj,
        nome_titular as razao_social,
        inscricao_municipal,

        logradouro as endereco_logradouro,
        numero as endereco_numero,
        complemento as endereco_complemento,
        cep as endereco_cep,
        bairro as endereco_bairro,
        null as endereco_cidade,

        case 
            when data_cancelamento is null 
                and data_revogacao is null 
                and data_anulacao is null 
                and data_cassacao is null
            then true 
            else false 
        end as ativo,
        situacao_do_alvara,
        situacao_da_emissao_da_licenca,
        situacao_da_licenca_sanitaria,
        situacao_validacao_da_licenca_sanitaria
    from {{ ref('raw_sisvisa__ambulante_sisvisa') }}
    where cpf is not null or cnpj is not null
),

obitos as (
    select 
        cpf.cpf
    from {{ ref('raw_bcadastro__cpf') }} as cpf
    where cpf.obito_ano is not null
),

ambulantes_no_sisvisa_atualizados as (
    select
        struct(
            'Ambulante' as tipo,
            cast(id as string) as id_sisvisa,
            cast(ambulantes_no_sisvisa.cpf as string) as cpf,
            cast(ambulantes_no_sisvisa.cnpj as string) as cnpj,
            cast(ambulantes_no_sisvisa.inscricao_municipal as string) as inscricao_municipal
        ) as identificacao,

        struct(
            razao_social as nome_empreendimento,
            cast(null as string) as natureza_juridica,
            cast(null as string) as porte,
            razao_social as titular,
            (obitos.cpf is not null) as titular_com_obito
        ) as cadastro,

        struct(
            ativo as sisvisa,
            cast(null as string) as receita_federal
        ) as atividade,

        struct(
            cast([] as array<string>) as tipos_operacoes,
            cast(endereco_bairro as string) as endereco_bairro,
            cast(endereco_cidade as string) as endereco_cidade
        ) as operacao,

        struct(
            cast(situacao_do_alvara as string) as alvara,
            cast(situacao_da_licenca_sanitaria as string) as licenca_sanitaria,
            cast(situacao_da_emissao_da_licenca as string) as licenca_sanitaria_emissao,
            cast(situacao_validacao_da_licenca_sanitaria as string) as licenca_sanitaria_validacao
        ) as situacao
    from ambulantes_no_sisvisa
        left join obitos on ambulantes_no_sisvisa.cpf = obitos.cpf
)

select * 
from ambulantes_no_sisvisa_atualizados

