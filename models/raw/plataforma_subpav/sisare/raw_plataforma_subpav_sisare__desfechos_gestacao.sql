{{ config(
    schema = 'brutos_plataforma_subpav',
    alias = 'sisare__desfechos_gestacao',
    materialized = 'table'
) }}

with source as (

    select *
    from {{ source('brutos_plataforma_subpav_staging', 'subpav_altas_referenciadas__desfechos_gestacao') }}

),

base as (

    select
        id_desfecho_gestacao,
        descricao,
        input,
        status,
        datalake_loaded_at
    from source

),

dados_limpos as (

    select
        {{ normalize_null("trim(id_desfecho_gestacao)") }} as id_desfecho_gestacao,
        {{ normalize_null("trim(descricao)") }} as descricao,
        {{ normalize_null("trim(input)") }} as input,
        {{ normalize_null("trim(status)") }} as status,
        {{ normalize_null("trim(datalake_loaded_at)") }} as datalake_loaded_at
    from base

),

deduplicado as (

    select *
    from dados_limpos
    qualify row_number() over (
        partition by
            id_desfecho_gestacao,
            descricao,
            input,
            status
        order by safe_cast(datalake_loaded_at as timestamp) desc
    ) = 1

)

select
    safe_cast(id_desfecho_gestacao as int64) as id_desfecho_gestacao,
    descricao,
    safe_cast(input as int64) as input,
    safe_cast(status as int64) as status,
    safe_cast(datalake_loaded_at as timestamp) as datalake_loaded_at
from deduplicado