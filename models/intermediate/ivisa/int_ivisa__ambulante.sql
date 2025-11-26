{{
    config(
        schema="intermediario_empreendimentos_cariocas",
        alias="int_ambulante",
        materialized="table",
    )
}}

with ambulantes_no_sisvisa as (
    select
        id,

        cpf,
        cnpj,
        {{ proper_br("nome_titular") }} as razao_social,
        inscricao_municipal,

        logradouro as endereco_logradouro,
        numero as endereco_numero,
        complemento as endereco_complemento,
        safe_cast(
            REGEXP_REPLACE(cep, r"[^0-9]", "")
            as int64
        ) as cep_int64, -- usado pra verificar se a cidade é o RJ
        
        -- Precisamos re-adicionar espaços antes de parênteses para
        -- "freguesia(jacarepaguá)"
        case
            when REGEXP_CONTAINS(bairro, r"[A-Za-z]\(")
                then array_to_string(split(bairro, "("), " (")
            else bairro
        end as endereco_bairro,

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

ambulantes_no_scca as (
    select
        null as id,

        cpf,
        cnpj,
        {{ proper_br("nome") }} as razao_social,
        inscricao_municipal,

        logradouro_ponto as endereco_logradouro,
        numero_porta_ponto as endereco_numero,
        complemento_ponto as endereco_complemento,
        null as cep_int64,
        case
            when REGEXP_CONTAINS(bairro_ponto, r"[A-Za-z]\(")
                then array_to_string(split(bairro_ponto, "("), " (")
            else bairro_ponto
        end as endereco_bairro,

        cast(null as boolean) as ativo,
        upper(trim(status)) as situacao_do_alvara,
        null as situacao_da_emissao_da_licenca,
        null as situacao_da_licenca_sanitaria,
        null as situacao_validacao_da_licenca_sanitaria
    from {{ ref("raw_sisvisa__ambulante_scca") }}
    where cpf is not null
        or cnpj is not null
),

ambulantes_unidos as (
    select *, "scca" as fonte from ambulantes_no_scca
    union all
    select *, "sisvisa" as fonte from ambulantes_no_sisvisa
),

obitos as (
    select
        cpf.cpf
    from {{ ref('raw_bcadastro__cpf') }} as cpf
    where cpf.obito_ano is not null
),

ambulantes_atualizados as (
    select
        struct(
            'Ambulante' as tipo,
            cast(id as string) as id_sisvisa,
            cast(ambulantes_unidos.cpf as string) as cpf,
            cast(ambulantes_unidos.cnpj as string) as cnpj,
            cast(ambulantes_unidos.inscricao_municipal as string) as inscricao_municipal
        ) as identificacao,

        struct(
            razao_social as nome_empreendimento,
            cast(null as string) as natureza_juridica,
            cast(null as string) as porte,
            razao_social as titular,
            (obitos.cpf is not null) as titular_com_obito,
            fonte
        ) as cadastro,

        struct(
            ativo as sisvisa,
            cast(null as string) as receita_federal
        ) as atividade,

        struct(
            cast([] as array<string>) as tipos_operacoes,
            {{ add_accents_bairros("endereco_bairro") }} as endereco_bairro,
            -- Confere se CEP é no Rio; se sim, aqui é "Rio de Janeiro",
            -- e se não, aqui é nulo
            cast(
                ({{ cidade_cep("cep_int64") }}) as string
            ) as endereco_cidade
        ) as operacao,

        struct(
            cast(situacao_do_alvara as string) as alvara,
            cast(situacao_da_licenca_sanitaria as string) as licenca_sanitaria,
            cast(situacao_da_emissao_da_licenca as string) as licenca_sanitaria_emissao,
            cast(situacao_validacao_da_licenca_sanitaria as string) as licenca_sanitaria_validacao
        ) as situacao
    from ambulantes_unidos
        left join obitos on ambulantes_unidos.cpf = obitos.cpf
)

select *
from ambulantes_atualizados
