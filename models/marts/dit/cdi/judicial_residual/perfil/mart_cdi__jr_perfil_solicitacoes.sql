{{ config(
    schema = "projeto_cdi",
    alias  = "jr_perfil_solicitacoes",
    materialized = "table",
    meta={"owner": "karen"}
) }}

with base as (

    select
        processo_rio,
        initcap(solicitacao) as tipo_solicitacao,
        initcap(orgao_para_subsidiar) as orgao_para_subsidiar,

        idade_categoria,
        sexo,

        entrada_gat3 as data_solicitacao,

        coalesce(
            initcap(situacao),
            'Não informado'
        ) as situacao

    from {{ ref('int_cdi__judicial_residual') }}
    where entrada_gat3 is not null

),

limpo as (

    select
        *,

        case 
            when sexo = 'F' then 'Feminino'
            when sexo = 'M' then 'Masculino'
            when sexo in ('F/M', 'M/F') then 'Ambos'
            else 'Não Informado'
        end as sexo_norm,

        case
            when idade_categoria = 'adulto' then 'Adulto'
            when idade_categoria = 'idoso' then 'Idoso'
            when idade_categoria = 'crianca' then 'Criança'
            when idade_categoria = 'adolescente' then 'Adolescente'
            when idade_categoria = 'rn' then 'Recém-nascido'
            when idade_categoria = 'nucleo_familiar' then 'Núcleo familiar'
            when idade_categoria = 'adulto e idoso' then 'Adulto e Idoso'
            else 'Não informado'
        end as faixa_etaria_norm

    from base

)

select
    processo_rio,
    tipo_solicitacao,
    orgao_para_subsidiar,
    sexo_norm as sexo,
    faixa_etaria_norm as faixa_etaria,
    data_solicitacao,
    situacao

from limpo

order by data_solicitacao