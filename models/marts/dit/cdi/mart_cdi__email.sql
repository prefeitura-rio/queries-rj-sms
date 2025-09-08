{{
    config(
        alias="email",
        schema="projeto_cdi",
        materialized="table",
    )
}}

with diario_municipal as (
    select distinct
        data_publicacao,
        'Diário Oficial do Município' as fonte,
        pasta,
        content_email,
        voto,
        link
    from {{ ref('int_cdi__diario_oficial_rj_filtrado') }}
),
diario_uniao as (
    select distinct
        data_publicacao,
        'Diário Oficial da União' as fonte,
        cast(null as string) as pasta,
        content_email,
        cast(null as string) as voto,
        link
    from {{ ref('int_cdi__diario_oficial_uniao_filtrado')}}
),
completo as (
    select * from diario_municipal
    union all
    select * from diario_uniao
),

-- Aqui calculamos o nº da edição automaticamente. A regra é:
-- * Todo novo envio de email é considerado uma nova edição;
-- * Se não houve envio (i.e. email vazio, flow deu erro, etc),
--   não pulamos edição.
-- Portanto, pegamos todas as datas distintas presentes nessa
-- tabela final (que estão diretamente conectadas aos emails enviados)
-- e somamos a um número de edição + data conhecidos.
-- No final, join com a tabela original.
-- Resultado: edições antigas serão 'null' (mas ninguém liga) e
-- novas edições, a princípio, serão incrementadas 1 a 1
datas as (
    select distinct data_publicacao
    from completo
),
edicoes as (
    select
        data_publicacao,
        (
            -- Dia 29/08/2025 foi edição nº304
            304 +
            row_number() over(order by data_publicacao asc)
        ) as edicao
    from datas
    where data_publicacao > '2025-08-29'
)

select
    *
from completo
left join edicoes
    using(data_publicacao)
