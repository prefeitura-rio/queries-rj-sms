{{
    config(
        schema='brutos_prontuario_prontuaRio',
        alias="paciente",
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
    select * from {{source('brutos_prontuario_prontuaRio_staging', 'paciente')}}
    {% if is_incremental() %} 
      where date(timestamp(loaded_at), 'America/Sao_Paulo') >= date( '{{ last_partition }}' )  
    {% endif %}
),

paciente as (
    select
        json_extract_scalar(data, '$.id_paciente') as id_paciente,
        json_extract_scalar(data, '$.registro') as registro,
        json_extract_scalar(data, '$.nome') as nome,
        json_extract_scalar(data, '$.data_nascimento') as data_nascimento,
        json_extract_scalar(data, '$.tipo_documento') as tipo_documento,
        json_extract_scalar(data, '$.peso') as peso,
        cnes,
        loaded_at
    from source_
),

final as (
    select 
        {{process_null('id_paciente')}} as id_paciente,        
        {{process_null('registro')}} as registro,
        {{process_null('nome')}} as nome,
        {{process_null('data_nascimento')}} as data_nascimento,
        {{process_null('tipo_documento')}} as tipo_documento,
        {{process_null('peso')}} as peso,
        cnes,
        datetime(timestamp(loaded_at), 'America/Sao_Paulo') as loaded_at,
        date(timestamp(loaded_at), 'America/Sao_Paulo') as data_particao
    from paciente
    qualify row_number() over(partition by id_paciente, registro, cnes order by loaded_at) = 1
)

select 
    concat(cnes, '.', id_paciente) as gid_paciente,
    concat(cnes, '.', registro) as gid_registro,
    *,
from final
