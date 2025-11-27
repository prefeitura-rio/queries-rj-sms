{{
    config(
        schema="intermediario_empreendimentos_cariocas",
        alias="int_estabelecimento",
        materialized="table",
    )
}}

with estabelecimentos_no_sisvisa as (
    select
        id,

        cnpj,
        {{ proper_br("razao_social") }} as razao_social,
        nome_fantasia as nome_fantasia,
        inscricao_municipal as inscricao_municipal,

        tipo_logradouro as endereco_logradouro_tipo,
        logradouro as endereco_logradouro,
        numero as endereco_numero,
        complemento as endereco_complemento,
        concat(cep_numero, cep_complemento) as endereco_cep,
        -- Precisamos re-adicionar espaços antes de parênteses para
        -- "freguesia(jacarepaguá)"
        case
            when REGEXP_CONTAINS(bairro, r"[A-Za-z]\(")
                then array_to_string(split(bairro, "("), " (")
            else bairro
        end as endereco_bairro,
        {{ clean_cidade("cidade") }} as endereco_cidade,

        ativo,
        case
            -- 01 - ATIVO
            when upper(situacao_do_alvara) like "%ATIVO"
                then "ATIVO"
            when trim(situacao_do_alvara) = "1"
                then "ATIVO"
            -- ?
            when upper(trim(situacao_do_alvara)) = "20 - INSCRICAO EX-OFFICIO"
                then "ATIVO"

            -- 40 - PROVISORIO CANCELADO
            -- 41 - CANCELADO
            -- 42 - CANCELADO DE OFICIO
            -- 50 - CANCELADO SEM PAGTO
            when upper(situacao_do_alvara) like "%CANCELADO%"
                then "CANCELADO"

            -- 00 - PENDENTE DE INCLUSAO
            -- 02 - PENDENTE DE ALTERACAO
            -- 08 - PENDENTE FIC
            -- 11 - PENDENTE DE ALVARA
            -- 15 - PENDENTE EMISSAO GUIA UNICA
            -- 25 - PENDENTE MICROEMPRESA
            -- 30 - PROVISORIO PENDENTE
            when upper(situacao_do_alvara) like "%PENDENTE%"
                then "PENDENTE"

            when upper(trim(situacao_do_alvara)) in (
                "06 - BAIXADO",
                "10 - BAIXA ISS",
                "21 - ANULADO",
                "22 - CASSADO",
                "23 - SUSPENSAO DE OFICIO",
                "28 - EX-OFFICIO/BAIXADO",
                "32 - PROVISORIO VENCIDO",
                "45 - CANC. BAIXA RFB",
                "46 - ALVARA SUSPENSO",
                "47 - CANC POR OBITO",
                "48 - BAIXA ISS OFICIO MEI"
            ) then "CANCELADO"

            when upper(trim(situacao_do_alvara)) in (
                "14 - SOLICITACAO GUIAS T.L.E.",
                "29 - PROVISORIO",
                "31 - PROVISORIO RENOVADO",
                "43 - PROV PRORR INDEFERIDA"
            ) then "PENDENTE"

            else null
        end as situacao_do_alvara,

        situacao_da_emissao_da_licenca,
        -- Licenciamento:
        case
            when situacao_da_licenca_sanitaria = 0 then cast(null as string)
            when situacao_da_licenca_sanitaria = 1 then "Autodeclarado"
            when situacao_da_licenca_sanitaria = 2 then "Simplificado"
            when situacao_da_licenca_sanitaria = 3 then "Licenciamento com Inspeção"
            when situacao_da_licenca_sanitaria = 4 then "Licenciamento por Autorização"
            when situacao_da_licenca_sanitaria = 5 then "Outorga"
            when situacao_da_licenca_sanitaria = 6 then "Licenciamento Manual"
            else trim(cast(situacao_da_licenca_sanitaria as string))
        end as situacao_da_licenca_sanitaria,
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
            cast(null as boolean) as titular_com_obito,
            "sisvisa" as fonte
        ) as cadastro,

        struct(
            ativo as sisvisa,
            situacao_cadastral as receita_federal
        ) as atividade,

        struct(
            formas_atuacao as tipos_operacoes,
            {{ add_accents_bairros("estabelecimentos_no_sisvisa.endereco_bairro") }} as endereco_bairro,
            {{ proper_br("estabelecimentos_no_sisvisa.endereco_cidade") }} as endereco_cidade
        ) as operacao,

        struct(
            cast(situacao_do_alvara as string) as alvara,
            cast(situacao_da_licenca_sanitaria as string) as licenca_sanitaria,
            cast(situacao_da_emissao_da_licenca as string) as licenca_sanitaria_emissao,
            cast(situacao_validacao_da_licenca_sanitaria as string) as licenca_sanitaria_validacao
        ) as situacao
    from estabelecimentos_no_sisvisa
    left join estabelecimentos_receita_federal
        on estabelecimentos_no_sisvisa.cnpj = estabelecimentos_receita_federal.cnpj
)

select *
from estabelecimentos_no_sisvisa_atualizados
