{{
    config(
        alias="empreendimentos",
        schema="projeto_empreendimentos_cariocas",
        materialized="table",
        partition_by={
          "field": "particao_cnpj",
          "data_type": "int64",
          "range": {"start": 0, "end": 99999999999, "interval": 2499999999975},
        }
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
        cadastros.cnpj.endereco.municipio_nome as endereco_cidade,

    from {{ ref('raw_bcadastro__cnpj') }} as cadastros
),

estabelecimentos_no_sisvisa_atualizados as (
    select
        cast(estabelecimentos_no_sisvisa.cnpj as int64) as particao_cnpj,

        struct(
            id as id_sisvisa,
            inscricao_municipal,
            estabelecimentos_no_sisvisa.cnpj
        ) as chaves,

        estabelecimentos_no_sisvisa.razao_social,
        estabelecimentos_no_sisvisa.nome_fantasia,

        estabelecimentos_receita_federal.natureza_juridica,
        formas_atuacao,
        porte,

        struct(
            ativo as sisvisa,
            situacao_cadastral as receita_federal
        ) as atividade,

        struct(
            estabelecimentos_no_sisvisa.endereco_logradouro_tipo,
            estabelecimentos_no_sisvisa.endereco_logradouro,
            estabelecimentos_no_sisvisa.endereco_numero,
            estabelecimentos_no_sisvisa.endereco_complemento,
            estabelecimentos_no_sisvisa.endereco_cep,
            estabelecimentos_no_sisvisa.endereco_bairro,
            estabelecimentos_no_sisvisa.endereco_cidade
        ) as endereco_sisvisa,

        struct(
            estabelecimentos_receita_federal.endereco_logradouro_tipo,
            estabelecimentos_receita_federal.endereco_logradouro,
            estabelecimentos_receita_federal.endereco_numero,
            estabelecimentos_receita_federal.endereco_complemento,
            estabelecimentos_receita_federal.endereco_cep,
            estabelecimentos_receita_federal.endereco_bairro,
            estabelecimentos_receita_federal.endereco_cidade
        ) as endereco_receita_federal,

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