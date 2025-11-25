{{
    config(
        schema="intermediario_empreendimentos_cariocas",
        alias="int_feirante",
        materialized="table",
    )
}}

with 

feirantes_no_slffe as (
    select
        null as id,

        cpf,
        cnpj,
        cast(null as string) as razao_social,
        inscricao_municipal,

        logradouro as endereco_logradouro,
        numero_porta as endereco_numero,
        complemento as endereco_complemento,
        cep as endereco_cep,
        bairro as endereco_bairro,
        municipio as endereco_cidade,

        case when situacao = 'ATIVO' then true else false end as ativo,
        cast(null as string) as situacao_do_alvara,
        cast(null as string) as situacao_da_emissao_da_licenca,
        cast(null as string) as situacao_da_licenca_sanitaria,
        cast(null as string) as situacao_validacao_da_licenca_sanitaria
    from {{ ref('raw_sisvisa__feirante_slffe') }}
),

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
        cast(situacao_do_alvara as string) as situacao_do_alvara,
        cast(situacao_da_emissao_da_licenca as string) as situacao_da_emissao_da_licenca,
        cast(situacao_da_licenca_sanitaria as string) as situacao_da_licenca_sanitaria,
        cast(situacao_validacao_da_licenca_sanitaria as string) as situacao_validacao_da_licenca_sanitaria
    from {{ ref('raw_sisvisa__feirante_sisvisa') }}
    where cpf is not null or cnpj is not null
),

feirantes_unidos as (
    select *, 'slffe' as fonte from feirantes_no_slffe
    union all
    select *, 'sisvisa' as fonte from feirantes_no_sisvisa
),

obitos as (
    select 
        cpf.cpf
    from {{ ref('raw_bcadastro__cpf') }} as cpf
    where cpf.obito_ano is not null
),

feirantes_atualizados as (
    select
        struct(
            'Feirante' as tipo,
            cast(id as string) as id_sisvisa,
            cast(feirantes.cpf as string) as cpf,
            cast(feirantes.cnpj as string) as cnpj,
            cast(feirantes.inscricao_municipal as string) as inscricao_municipal
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
    from feirantes_unidos feirantes
        left join obitos on feirantes.cpf = obitos.cpf
)

select * 
from feirantes_atualizados

