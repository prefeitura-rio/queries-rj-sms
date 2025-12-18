{{
    config(
        schema='brutos_prontuario_prontuaRio',
        alias="paciente",
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
    select * from {{source('brutos_prontuario_prontuaRio_staging', 'paciente')}}
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
        cast(loaded_at as timestamp) as loaded_at
    from paciente
)

select 
    concat(cnes, '.', id_paciente) as gid_paciente,
    concat(cnes, '.', registro) as gid_registro,
    *,
    date(loaded_at) as data_particao
from final
