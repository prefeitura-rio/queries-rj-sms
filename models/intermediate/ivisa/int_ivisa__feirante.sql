{{
    config(
        schema="intermediario_empreendimentos_cariocas",
        alias="int_feirante",
        materialized="table",
    )
}}

with 

feirantes_no_sisvisa as (
    select
        id,

        cpf,
        cnpj,
        razao_social,
        inscricao_municipal,

        logradouro as endereco_logradouro,
        numero_porta as endereco_numero,
        complemento as endereco_complemento,
        cep as endereco_cep,
        bairro as endereco_bairro,
        municipio as endereco_cidade,

        case when afastado = 'N' then true else false end as ativo,
        situacao_do_alvara,
        situacao_da_emissao_da_licenca,
        situacao_da_licenca_sanitaria,
        situacao_validacao_da_licenca_sanitaria
    from {{ ref('raw_sisvisa__feirante') }}
    where cpf is not null or cnpj is not null
),

obitos as (
    select 
        cpf.cpf
    from {{ ref('raw_bcadastro__cpf') }} as cpf
    where cpf.obito_ano is not null
),

feirantes_no_sisvisa_atualizados as (
    select
        struct(
            'Feirante' as tipo,
            id as id_sisvisa,
            feirantes_no_sisvisa.cpf,
            cnpj,
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
    from feirantes_no_sisvisa
        left join obitos on feirantes_no_sisvisa.cpf = obitos.cpf
)

select * 
from feirantes_no_sisvisa_atualizados

