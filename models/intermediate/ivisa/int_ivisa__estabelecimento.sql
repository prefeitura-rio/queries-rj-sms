{{
    config(
        schema="intermediario_empreendimentos_cariocas",
        alias="int_estabelecimento",
        materialized="table",
    )
}}

with 

estabelecimentos_no_sisvisa as (
    select
        id,

        cnpj,
        razao_social as razao_social,
        nome_fantasia as nome_fantasia,
        inscricao_municipal as inscricao_municipal,

        tipo_logradouro as endereco_logradouro_tipo,
        logradouro as endereco_logradouro,
        numero as endereco_numero,
        complemento as endereco_complemento,
        concat(cep_numero, cep_complemento) as endereco_cep,
        bairro as endereco_bairro,
        cidade as endereco_cidade,

        ativo,
        situacao_do_alvara,
        situacao_da_emissao_da_licenca,
        situacao_da_licenca_sanitaria,
        situacao_validacao_da_licenca_sanitaria
    from {{ ref('raw_sisvisa__estabelecimento') }}
    where cnpj is not null
),

estabelecimentos_receita_federal as (
    select 
        cadastros.cnpj.cnpj,
        cadastros.cnpj.razao_social,
        cadastros.cnpj.nome_fantasia,
        cadastros.cnpj.natureza_juridica.descricao as natureza_juridica,
        cadastros.cnpj.porte.descricao as porte,
        cadastros.cnpj.situacao_cadastral.descricao as situacao_cadastral,
        cadastros.cnpj.formas_atuacao,
        cadastros.cnpj.endereco.id_municipio,

        cadastros.cnpj.endereco.tipo_logradouro as endereco_logradouro_tipo,
        cadastros.cnpj.endereco.logradouro as endereco_logradouro,
        cadastros.cnpj.endereco.numero as endereco_numero,
        cadastros.cnpj.endereco.complemento as endereco_complemento,
        cadastros.cnpj.endereco.cep as endereco_cep,
        cadastros.cnpj.endereco.bairro as endereco_bairro,
        cadastros.cnpj.endereco.municipio_nome as endereco_cidade

    from {{ ref('raw_bcadastro__cnpj') }} as cadastros
),

estabelecimentos_no_sisvisa_atualizados as (
    select
        struct(
            'Estabelecimento' as tipo,
            cast(id as string) as id_sisvisa,
            cast(null as string) as cpf,
            cast(estabelecimentos_no_sisvisa.cnpj as string) as cnpj,
            cast(estabelecimentos_no_sisvisa.inscricao_municipal as string) as inscricao_municipal
        ) as identificacao,

        struct(
            estabelecimentos_no_sisvisa.razao_social as nome_empreendimento,
            estabelecimentos_receita_federal.natureza_juridica,
            porte,
            cast(null as string) as titular,
            cast(null as boolean) as titular_com_obito
        ) as cadastro,

        struct(
            ativo as sisvisa,
            situacao_cadastral as receita_federal
        ) as atividade,

        struct(
            formas_atuacao as tipos_operacoes,
            estabelecimentos_no_sisvisa.endereco_bairro,
            estabelecimentos_no_sisvisa.endereco_cidade
        ) as operacao,

        struct(
            cast(situacao_do_alvara as string) as alvara,
            cast(situacao_da_licenca_sanitaria as string) as licenca_sanitaria,
            cast(situacao_da_emissao_da_licenca as string) as licenca_sanitaria_emissao,
            cast(situacao_validacao_da_licenca_sanitaria as string) as licenca_sanitaria_validacao
        ) as situacao
    from estabelecimentos_no_sisvisa left join estabelecimentos_receita_federal
        on estabelecimentos_no_sisvisa.cnpj = estabelecimentos_receita_federal.cnpj
)

select * 
from estabelecimentos_no_sisvisa_atualizados