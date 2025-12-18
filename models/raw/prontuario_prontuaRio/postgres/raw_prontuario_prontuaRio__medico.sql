{{
    config(
        schema='brutos_prontuario_prontuaRio',
        alias="medico",
        materialized="table",
        tags=["prontuaRio"],
        partition_by={
            "field": "data_particao",
            "data_type": "date",
            "granularity": "day",
        },      
    )
}}

with 
source_ as (
    select * from {{source('brutos_prontuario_prontuaRio_staging', 'medico')}}
),

medico as (
    select
        json_extract_scalar(data, '$.nome') as nome,
        json_extract_scalar(data, '$.crm') as crm,
        json_extract_scalar(data, '$.login') as login,
        json_extract_scalar(data, '$.senha') as senha,
        json_extract_scalar(data, '$.ativo') as ativo,
        json_extract_scalar(data, '$.cpf') as cpf,
        cnes,
        loaded_at
    from source_
),

final as (
    select 
    {{process_null("cpf")}} as cpf,
    {{process_null("nome")}} as nome,
    {{process_null("crm")}} as crm,
    {{process_null("ativo")}} as ativo,
    cnes,
    cast(loaded_at as timestamp) as loaded_at
    from medico
)


select 
    *,
    cast(loaded_at as date) as data_particao 
from final