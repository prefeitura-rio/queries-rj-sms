{{
    config(
        schema='brutos_prontuario_prontuaRio',
        alias="prontuario_be",
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
    select * from {{source('brutos_prontuario_prontuaRio_staging', 'hp_prontuario_be') }} 
    {% if is_incremental() %} 
      where cast(loaded_at as date) >= date( '{{ last_partition }}' ) 
    {% endif %}
  ),

  prontuario_be as (
    select
      json_extract_scalar(data, '$.prontuario') as id_prontuario,
      json_extract_scalar(data, '$.be') as id_boletim,
      cnes,
      loaded_at
    from source_
  ),

    final as (
      select 
        safe_cast(id_prontuario as int64) as id_prontuario,
        safe_cast(id_boletim as int64) as id_boletim,
        cnes,
        loaded_at,
        cast(loaded_at as date) as data_particao
      from prontuario_be 
      qualify row_number() over(partition by id_prontuario, id_boletim, cnes order by loaded_at desc) = 1
    )

select
  concat(cnes, '.', id_prontuario) as gid_prontuario,
  concat(cnes, '.', id_boletim) as gid_boletim,
  *
from final