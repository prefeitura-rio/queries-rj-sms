{{
    config(
        schema='brutos_prontuario_prontuaRio',
        alias="medico",
        materialized="incremental",
        incremental_strategy="insert_overwrite",
        tags=["prontuaRio"],
        partition_by={
            "field": "data_particao",
            "data_type": "date",
            "granularity": "day",
        },      
    )
}}

{% set last_partition = get_last_partition_date(this) %}

with 
source_ as (
    select * from {{source('brutos_prontuario_prontuaRio_staging', 'medico')}}
    {% if is_incremental() %} 
      where date(timestamp(loaded_at), 'America/Sao_Paulo') >= date( '{{ last_partition }}' ) 
    {% endif %}
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
    datetime(timestamp(loaded_at), 'America/Sao_Paulo') as loaded_at,
    date(timestamp(loaded_at), 'America/Sao_Paulo') as data_particao
    from medico
    qualify row_number() over(partition by cpf, cnes order by loaded_at) = 1
)


select 
    concat(cnes, '.', cpf) as gid_medico,
    *,
from final