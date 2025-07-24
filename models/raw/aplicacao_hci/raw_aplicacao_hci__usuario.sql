{{

    config(
        alias="usuario",
        materialized="table"
    )
}}
with source as (
select * from {{source('brutos_aplicacao_hci_staging', 'public__user')}}
)

select 
    cast(id as string) as id,
    cast(username as string) as username,
    case 
        when is_active = 'True' then true
        when is_active = 'False' then false
        else null
    end as is_active,
    case 
        when is_superuser = 'True' then true
        when is_superuser = 'False' then false
        else null
    end as is_superuser,
    cast(created_at as datetime) as created_at,
    cast(updated_at as datetime) as updated_at,
    upper(cast(name as string)) as nome,
    cpf,
    cast(use_terms_accepted_at as datetime) as aceita_termos_datahora,
        case 
        when is_use_terms_accepted = 'True' then true
        when is_use_terms_accepted = 'False' then false
        else null
    end as aceita_termos,
    {{process_null('password')}} as senha,
    {{process_null('password')}} as telefone,
    {{process_null('email')}} as email,
    cast(loaded_at as datetime) as loaded_at,
    ano_particao,
    mes_particao,
    data_particao,
from source