{{
    config(
        schema='brutos_prontuario_prontuaRio',
        alias="ocupacao",
        materialized="table",
        tags=["prontuaRio"],
    )
}}

with 
    source_ as (
        select * from {{ source('brutos_prontuario_prontuaRio_staging', 'intb0') }}
    ),

    ocupacoes as (
        select 
            json_extract_scalar(data, '$.ib0codigo') as id_cbo,
            json_extract_scalar(data, '$.ib0descr') as descricao,
            safe_cast(loaded_at as timestamp) as loaded_at
        from source_
    )

select * from ocupacoes
qualify row_number() over(partition by id_cbo, descricao order by loaded_at desc)=1
