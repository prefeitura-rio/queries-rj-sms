{{
    config(
        schema='brutos_prontuario_prontuaRio',
        alias="prontuario_be",
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
    select * from {{source('brutos_prontuario_prontuaRio_staging', 'hp_prontuario_be') }} 
  ),

  prontuario_be as (
    select
      json_extract_scalar(data, '$.prontuario') as id_prontuario,
      json_extract_scalar(data, '$.be') as id_boletim,
      cnes,
      loaded_at
    from source_
  ),

    deduplicated as (
      select * from prontuario_be 
      qualify row_number() over(partition by id_prontuario, id_boletim, cnes order by loaded_at desc) = 1
    )

select 
  *, 
  cast(safe_cast(loaded_at as timestamp) as date) as data_particao
from deduplicated