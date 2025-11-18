{{
    config(
        schema="intermediario_empreendimentos_cariocas",
        alias="int_banca_jornal",
        materialized="table",
    )
}}

with 

bancas_no_sisvisa as (
    select
        id,

        cpf_titular as cpf,
        nome_titular as razao_social,
        inscricao_municipal,

        logradouro_banca as endereco_logradouro,
        numero_porta_banca as endereco_numero,
        complemento_banca as endereco_complemento,
        cep_banca as endereco_cep,
        bairro_banca as endereco_bairro,
        null as endereco_cidade,

        ativo,
        situacao_do_alvara,
        situacao_da_emissao_da_licenca,
        situacao_da_licenca_sanitaria,
        situacao_validacao_da_licenca_sanitaria
    from {{ ref('raw_sisvisa__banca_jornal') }}
    where cpf_titular is not null
),

obitos as (
    select 
        cpf.cpf
    from {{ ref('raw_bcadastro__cpf') }} as cpf
    where cpf.obito_ano is not null
),

bancas_no_sisvisa_atualizadas as (
    select
        struct(
            'Banca de Jornal' as tipo,
            id as id_sisvisa,
            bancas_no_sisvisa.cpf,
            null as cnpj,
            inscricao_municipal
        ) as identificacao,

        struct(
            razao_social as nome_empreendimento,
            null as natureza_juridica,
            null as porte,
            razao_social as titular,
            (obitos.cpf is not null) as titular_com_obito
        ) as cadastro,

        struct(
            ativo as sisvisa,
            null as receita_federal
        ) as atividade,

        struct(
            null as atividades,
            null as tipos_operacoes,
            endereco_bairro,
            endereco_cidade
        ) as operacao,

        struct(
            cast(situacao_do_alvara as string) as alvara,
            cast(situacao_da_licenca_sanitaria as string) as licenca_sanitaria,
            cast(situacao_da_emissao_da_licenca as string) as licenca_sanitaria_emissao,
            cast(situacao_validacao_da_licenca_sanitaria as string) as licenca_sanitaria_validacao
        ) as situacao
    from bancas_no_sisvisa
        left join obitos on bancas_no_sisvisa.cpf = obitos.cpf
)

select * 
from bancas_no_sisvisa_atualizadas

