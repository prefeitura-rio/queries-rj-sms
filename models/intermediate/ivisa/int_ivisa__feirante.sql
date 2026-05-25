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
        {{ clean_bairro("bairro") }} as endereco_bairro,
        {{ clean_cidade("municipio") }} as endereco_cidade,

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
        {{ proper_br("razao_social") }} as razao_social,
        inscricao_municipal,

        logradouro as endereco_logradouro,
        numero_porta as endereco_numero,
        complemento as endereco_complemento,
        cep as endereco_cep,
        {{ clean_bairro("bairro") }} as endereco_bairro,
        {{ clean_cidade("municipio") }} as endereco_cidade,

        case when afastado = 'N' then true else false end as ativo,
        case 
            when upper(trim(situacao_do_alvara)) = "BAIXADO"
                then "CANCELADO"
            else upper(trim(situacao_do_alvara))
        end as situacao_do_alvara,

        cast(situacao_da_emissao_da_licenca as string) as situacao_da_emissao_da_licenca,
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
        cast(situacao_validacao_da_licenca_sanitaria as string) as situacao_validacao_da_licenca_sanitaria
    from {{ ref('raw_sisvisa__feirante_sisvisa') }}
    where cpf is not null or cnpj is not null
),

feirantes_unidos as (
    select *, 'slffe' as fonte from feirantes_no_slffe
    union all
    select *, 'sisvisa' as fonte from feirantes_no_sisvisa
),

feirantes_deduplicados as (
    -- Vide explicação em `int_ivisa__ambulante`
    select
        coalesce(max(id), max(id)) as id,
        coalesce(max(cpf), max(cpf)) as cpf,
        coalesce(max(cnpj), max(cnpj)) as cnpj,
        coalesce(max(razao_social), max(razao_social)) as razao_social,
        inscricao_municipal,
        coalesce(max(endereco_logradouro), max(endereco_logradouro)) as endereco_logradouro,
        coalesce(max(endereco_numero), max(endereco_numero)) as endereco_numero,
        coalesce(max(endereco_complemento), max(endereco_complemento)) as endereco_complemento,
        coalesce(max(endereco_cep), max(endereco_cep)) as endereco_cep,
        coalesce(max(endereco_bairro), max(endereco_bairro)) as endereco_bairro,
        coalesce(max(endereco_cidade), max(endereco_cidade)) as endereco_cidade,
        coalesce(max(ativo), max(ativo)) as ativo,
        coalesce(max(situacao_do_alvara), max(situacao_do_alvara)) as situacao_do_alvara,
        coalesce(max(situacao_da_emissao_da_licenca), max(situacao_da_emissao_da_licenca)) as situacao_da_emissao_da_licenca,
        coalesce(max(situacao_da_licenca_sanitaria), max(situacao_da_licenca_sanitaria)) as situacao_da_licenca_sanitaria,
        coalesce(max(situacao_validacao_da_licenca_sanitaria), max(situacao_validacao_da_licenca_sanitaria)) as situacao_validacao_da_licenca_sanitaria,
        min(fonte) as fonte  -- prefere 'sisvisa' a 'slffe' se for o caso
    from feirantes_unidos
    group by inscricao_municipal
),

obitos as (
    select bcadastro.cpf
    from {{ ref("raw_bcadastro__cpf") }} as bcadastro
    where bcadastro.obito_ano is not null

    union distinct

    select cpf
    from {{ ref("int_historico_clinico__paciente__vitacare") }},
        unnest(dados) as dado
    where cpf is not null
        and dado.rank = 1
        and dado.obito_indicador = true

    union distinct

    select cpf
    from {{ ref("int_historico_clinico__paciente__smsrio") }},
        unnest(dados) as dado
    where cpf is not null
        and dado.rank = 1
        and dado.obito_indicador = true

    union distinct

    select cpf
    from {{ ref("int_historico_clinico__paciente__vitai") }},
        unnest(dados) as dado
    where cpf is not null
        and dado.rank = 1
        and dado.obito_indicador = true
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
            (obitos.cpf is not null) as titular_com_obito,
            fonte
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
    from feirantes_deduplicados as feirantes
        left join obitos on feirantes.cpf = obitos.cpf
)

select * 
from feirantes_atualizados

