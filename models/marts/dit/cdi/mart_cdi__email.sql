{{
    config(
        alias="email",
        schema="projeto_cdi",
        materialized="incremental",
        tags=["cdi_vps"],
        partition_by={
            "field": "data_publicacao",
            "data_type": "date"
        }
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
    -- (1) Se a tabela JÁ EXISTE e vamos adicionar mais linhas nela
    {% if is_incremental() %}
        select
            data_publicacao,
            (
                -- (1.2) Pegamos a maior edição já na tabela, e incrementamos
                --       a partir desse valor
                (select max(edicao) from {{ this }}) +
                row_number() over(order by data_publicacao asc)
            ) as edicao
        from datas
        -- (1.1) Filtramos por edições que sejam mais recentes do que já na tabela
        where data_publicacao > (select max(data_publicacao) from {{ this }})
    -- (2) Senão, se a tabela vai ser materializada do zero
    {% else %}
        select
            data_publicacao,
            (
                -- (2.2) Dia 31/12/2025 foi edição nº372
                372 +
                row_number() over(order by data_publicacao asc)
            ) as edicao
        from datas
        -- (2.1) Fazemos a conta a partir de uma data em que sabemos a edição
        where data_publicacao > '2025-12-31'
    {% endif %}
)

select
    *
from completo
left join edicoes
    using(data_publicacao)
{% if is_incremental() %}
where data_publicacao > (select max(data_publicacao) from {{ this }})
{% endif %}
