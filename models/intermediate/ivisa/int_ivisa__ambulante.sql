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
        cast(null as string) as situacao_da_licenca_sanitaria,
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

ambulantes_deduplicados as (
    -- Queremos deduplicar mas sem substituição
    -- Isto é, queremos juntar entradas com mesma inscrição municipal
    -- removendo todos os nulos possíveis. Para isso, fazemos coalesce(max, max).
    -- Em duas repetições, se um é nulo, o outro é o max.
    -- Contudo, na base com ~300 mil linhas, 6 possuem 3 repetições.
    -- Assim, estamos em teoria perdendo algumas poucas informações caso
    -- mais de um esteja preenchido com não-nulo :\
    -- [Ref] https://www.rainstormtech.com/efficient-data-deduplication-and-null-value-handling-in-sql-server/
    select
        coalesce(max(id), max(id)) as id,
        coalesce(max(cpf), max(cpf)) as cpf,
        coalesce(max(cnpj), max(cnpj)) as cnpj,
        coalesce(max(razao_social), max(razao_social)) as razao_social,
        inscricao_municipal,
        coalesce(max(endereco_logradouro), max(endereco_logradouro)) as endereco_logradouro,
        coalesce(max(endereco_numero), max(endereco_numero)) as endereco_numero,
        coalesce(max(endereco_complemento), max(endereco_complemento)) as endereco_complemento,
        coalesce(max(cep_int64), max(cep_int64)) as cep_int64,
        coalesce(max(endereco_bairro), max(endereco_bairro)) as endereco_bairro,
        coalesce(max(ativo), max(ativo)) as ativo,
        coalesce(max(situacao_do_alvara), max(situacao_do_alvara)) as situacao_do_alvara,
        coalesce(max(situacao_da_emissao_da_licenca), max(situacao_da_emissao_da_licenca)) as situacao_da_emissao_da_licenca,
        coalesce(max(situacao_da_licenca_sanitaria), max(situacao_da_licenca_sanitaria)) as situacao_da_licenca_sanitaria,
        coalesce(max(situacao_validacao_da_licenca_sanitaria), max(situacao_validacao_da_licenca_sanitaria)) as situacao_validacao_da_licenca_sanitaria,
        max(fonte) as fonte  -- prefere 'sisvisa' a 'scca' se for o caso
    from ambulantes_unidos
    group by inscricao_municipal
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
            cast(ambulantes.cpf as string) as cpf,
            cast(ambulantes.cnpj as string) as cnpj,
            cast(ambulantes.inscricao_municipal as string) as inscricao_municipal
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
    from ambulantes_deduplicados as ambulantes
        left join obitos on ambulantes.cpf = obitos.cpf
)

select *
from ambulantes_atualizados
