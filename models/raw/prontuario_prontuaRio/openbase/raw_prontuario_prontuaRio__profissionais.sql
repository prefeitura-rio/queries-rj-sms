{{
    config(
        schema='brutos_prontuario_prontuaRio',
        alias="profissionais",
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
  select * from {{ source('brutos_prontuario_prontuaRio_staging', 'intc0') }}
),

profissionais as (
  select
        json_extract_scalar(data, '$.ic0cpf') as cpf,
        json_extract_scalar(data, '$.ic0cns') as cns,
        json_extract_scalar(data, '$.ic0matricula') as matricula,
        json_extract_scalar(data, '$.ic0nome') as nome_completo,
        json_extract_scalar(data, '$.ic0icr') as icr,
        json_extract_scalar(data, '$.ic0nomgue') as nome_guerra,
        json_extract_scalar(data, '$.ic0telef') as telefone,
        json_extract_scalar(data, '$.ic0codocup') as id_cbo,
        json_extract_scalar(data, '$.ic0atua') as atua,
        json_extract_scalar(data, '$.ic0ativ') as ativ,
        json_extract_scalar(data, '$.ic0flagativo') as ativo_indicador,
        json_extract_scalar(data, '$.ic0email') as email,
        cnes,
        loaded_at
  from source_
),

final as (
    select 
        {{process_null('cpf')}} as cpf,        
        {{process_null('cns')}} as cns,
        {{process_null('matricula')}} as matricula,
        {{process_null('nome_completo')}} as nome_completo,
        {{process_null('icr')}} as icr,
        {{process_null('nome_guerra')}} as nome_guerra,
        {{process_null('telefone')}} as telefone,
        rpad(id_cbo, 6, '0') as id_cbo,
        {{process_null('atua')}} as atua,
        {{process_null('ativ')}} as ativ,
        {{process_null('ativo_indicador')}} as ativo_indicador,
        {{process_null('email')}} as email,
        cnes,
        cast(cast(loaded_at as timestamp)as datetime) as loaded_at
    from profissionais
    qualify row_number() over(partition by cpf, cnes order by loaded_at desc) = 1 

)


select 
    *,
    cast(loaded_at as date) as data_particao,
from final


