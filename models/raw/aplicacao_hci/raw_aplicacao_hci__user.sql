{{
    config(
        alias="usuario",
        tags="aplicacao_hci"
    )
}}
with 
    source as (
        select * from {{ source('brutos_aplicacao_hci_staging','public__user') }}
    ),
    most_recent as (
        select * from source
        qualify row_number() over (partition by cpf order by updated_at desc) = 1
    ),
    casted as (
        select
            id,
            name as nome,
            cpf,
            email,
            safe_cast(is_active as boolean) as indicador_ativo,
            safe_cast(is_superuser as boolean) as indicador_superusuario,
            {{process_null('cnes')}} as cnes,
            safe_cast(use_terms_accepted_at as timestamp) as data_aceite_termos_uso,
            safe_cast(is_use_terms_accepted as boolean) as indicador_aceite_termos_uso,
            {{process_null('ap')}} as ap,
            {{process_null('access_level')}} as nivel_acesso,
            {{process_null('job_title')}} as cargo,
            safe_cast(created_at as timestamp) as created_at,
            safe_cast(updated_at as timestamp) as updated_at,
            safe_cast(datalake_loaded_at as timestamp) as loaded_at
        from most_recent
    )
select * 
from casted