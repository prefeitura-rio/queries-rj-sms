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
    end as eh_ativado,
    case 
        when is_superuser = 'True' then true
        when is_superuser = 'False' then false
        else null
    end as eh_superuser,
    SAFE_CAST(created_at as timestamp) as created_at,
    SAFE_CAST(updated_at as timestamp) as updated_at,
    upper(cast(name as string)) as nome,
    cpf,
    SAFE_CAST(use_terms_accepted_at as timestamp) as aceita_termos_datahora,
        case 
        when is_use_terms_accepted = 'True' then true
        when is_use_terms_accepted = 'False' then false
        else null
    end as aceita_termos,
    {{process_null('password')}} as senha,
    {{process_null('password')}} as telefone,
    {{process_null('email')}} as email,
    SAFE_CAST(loaded_at as timestamp) as loaded_at,
    ano_particao,
    mes_particao,
    data_particao
from source