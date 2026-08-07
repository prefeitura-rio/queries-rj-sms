{{
    config(
        schema = 'brutos_basedosdados_br',
        alias = 'municipio_rio',
        materialized = 'table',
        tags = ['basedosdados', 'ibge_populacao']
    )
}}

select
    cast(ano as int64) as ano,
    cast(id_municipio as string) as id_municipio,
    cast(populacao as int64) as populacao,

    'basedosdados.br_ibge_populacao.municipio' as fonte,
    current_timestamp() as loaded_at

from {{ source('basedosdados_br_ibge_populacao', 'municipio') }}

where id_municipio = '3304557'
