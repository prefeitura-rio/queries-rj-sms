{{

    config(
        alias="historico",
        materialized="table"
    )
}}
with source as (
select * from {{ source('brutos_aplicacao_hci_staging', 'public__userhistory') }}
)
select 
    cast(id as string) as id,	
    cast(method as string) as method,	
    cast(path as string) as path,	
    query_params,
    {{process_null('body')}} as body,	
    cast(status_code as int64) as status_code,	
    cast(timestamp as datetime) as created_at,
    cast(user_id as string) as usuario_id,
    cast(loaded_at as datetime) as loaded_at,
    ano_particao,
    mes_particao,
    data_particao,
from source