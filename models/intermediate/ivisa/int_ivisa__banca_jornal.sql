{{
    config(
        schema="intermediario_empreendimentos_cariocas",
        alias="int_banca_jornal",
        materialized="table",
    )
}}

with bancas_no_sisvisa as (
    select
        id,

        cpf_titular as cpf,
        {{ proper_br("nome_titular") }} as razao_social,
        inscricao_municipal,

        logradouro_banca as endereco_logradouro,
        numero_porta_banca as endereco_numero,
        complemento_banca as endereco_complemento,
        cep_banca as endereco_cep,
        {{ clean_bairro("bairro_banca") }} as endereco_bairro,
        cast(null as string) as endereco_cidade,

        ativo,
        case
            when upper(trim(situacao_do_alvara)) = "ATIVA"
                then "ATIVO"
            when upper(trim(situacao_do_alvara)) in (
                "BAIXADA", "CANCELADA"
            ) then "CANCELADO"
            when upper(trim(situacao_do_alvara)) = "PENDENTE"
                then "PENDENTE"
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
    from {{ ref('raw_sisvisa__banca_jornal_sisvisa') }}
    where cpf_titular is not null
),

-- SILFAE tem muito pouca informação :\
bancas_no_silfae as (
    select
        cast(null as int64) as id,

        cpf_titular as cpf,
        {{ proper_br("nome_titular") }} as razao_social,
        cast(inscricao_municipal as string) as inscricao_municipal,

        logradouro_banca as endereco_logradouro,
        numero_porta_banca as endereco_numero,
        complemento_banca as endereco_complemento,
        cast(null as string) as endereco_cep,
        {{ clean_bairro("bairro_banca") }} as endereco_bairro,
        cast(null as string) as endereco_cidade,

        cast(null as boolean) as ativo,
        case
            when upper(trim(situacao)) = "ATIVA"
                then "ATIVO"
            when upper(trim(situacao)) in (
                "BAIXADA", "CANCELADA"
            ) then "CANCELADO"
            when upper(trim(situacao)) = "PENDENTE"
                then "PENDENTE"
            else null
        end as situacao_do_alvara,
        null as situacao_da_emissao_da_licenca,
        cast(null as string) as situacao_da_licenca_sanitaria,
        null as situacao_validacao_da_licenca_sanitaria
    from {{ ref("raw_sisvisa__banca_jornal_silfae") }}
    where cpf_titular is not null
),

bancas_unidas as (
    select *, "silfae" as fonte from bancas_no_silfae
    union all
    select *, "sisvisa" as fonte from bancas_no_sisvisa
),

bancas_deduplicadas as (
    -- Vide explicação em `int_ivisa__ambulante`
    select
        coalesce(max(id), max(id)) as id,
        coalesce(max(cpf), max(cpf)) as cpf,
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
        max(fonte) as fonte  -- prefere 'sisvisa' a 'silfae' se for o caso
    from bancas_unidas
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

bancas_atualizadas as (
    select
        struct(
            'Banca de Jornal' as tipo,
            cast(id as string) as id_sisvisa,
            cast(bancas.cpf as string) as cpf,
            cast(null as string) as cnpj,
            cast(bancas.inscricao_municipal as string) as inscricao_municipal
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
    from bancas_deduplicadas as bancas
        left join obitos on bancas.cpf = obitos.cpf
)

select *
from bancas_atualizadas
