{{
    config(
        schema='brutos_prontuario_prontuaRio',
        alias="prescricao",
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
    select * from {{source('brutos_prontuario_prontuaRio_staging', 'prescricao')}}
),

prescricao as (
    select
        json_extract_scalar(data, '$.id_prescricao') as id_prescricao,
        json_extract_scalar(data, '$.id_atendimento') as id_atendimento,
        json_extract_scalar(data, '$.impressao_farmacia') as impressao_farmacia,
        json_extract_scalar(data, '$.impressao_nutricao') as impressao_nutricao,
        json_extract_scalar(data, '$.status_impressao') as status_impressao,
        json_extract_scalar(data, '$.aprasado') as aprasado,
        json_extract_scalar(data, '$.etiqueta_aprasar_impressa') as etiqueta_aprasar_impressa,
        json_extract_scalar(data, '$.ativo_aprasamento') as ativo_aprasamento,
        json_extract_scalar(data, '$.status_aprasado') as status_aprasado,
        cnes,
        loaded_at
    from source_
),

final as (
    select 
        {{ process_null('id_prescricao') }} as id_prescricao,
        {{ process_null('id_atendimento') }} as id_atendimento,
        {{ process_null('impressao_farmacia') }} as impressao_farmacia,
        {{ process_null('impressao_nutricao') }} as impressao_nutricao,
        {{ process_null('status_impressao') }} as status_impressao,
        {{ process_null('aprasado') }} as aprasado,
        {{ process_null('etiqueta_aprasar_impressa') }} as etiqueta_aprasar_impressa,
        {{ process_null('ativo_aprasamento') }} as ativo_aprasamento,
        {{ process_null('status_aprasado') }} as status_aprasado,
        cnes,
        loaded_at
    from prescricao
    qualify row_number() over(partition by id_prescricao, id_atendimento, cnes order by loaded_at desc) = 1
)

select 
    *, 
    date(safe_cast(loaded_at as timestamp)) as data_particao 
from final
