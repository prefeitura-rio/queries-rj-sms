{{ config(
    schema = 'brutos_plataforma_subpav',
    alias = 'gestantes',
    materialized = 'table'
) }}

with source as (

    select *
    from {{ source('brutos_plataforma_subpav_staging', 'subpav_cegonha__gestantes') }}

),

base as (

    select
        id_gestante,
        num_cns,
        cpf,
        nme_nome,
        flg_ativo,
        created,
        modified,
        id_user,
        id_raca_cor,
        num_cns_video,
        datalake_loaded_at
    from source

),

dados_limpos as (

    select
        {{ normalize_null("trim(id_gestante)") }} as id_gestante,
        {{ normalize_null("trim(num_cns)") }} as num_cns,
        {{ normalize_null("trim(cpf)") }} as cpf,
        {{ normalize_null("trim(nme_nome)") }} as nme_nome,
        {{ normalize_null("trim(flg_ativo)") }} as flg_ativo,
        {{ normalize_null("trim(created)") }} as created,
        {{ normalize_null("trim(modified)") }} as modified,
        {{ normalize_null("regexp_replace(trim(id_user), r'\\.0$', '')") }} as id_user,
        {{ normalize_null("trim(id_raca_cor)") }} as id_raca_cor,
        {{ normalize_null("trim(num_cns_video)") }} as num_cns_video,
        {{ normalize_null("trim(datalake_loaded_at)") }} as datalake_loaded_at
    from base

),

deduplicado as (

    select *
    from dados_limpos
    qualify row_number() over (
        partition by
            id_gestante,
            num_cns,
            cpf,
            nme_nome,
            flg_ativo,
            created,
            modified,
            id_user,
            id_raca_cor,
            num_cns_video
        order by safe_cast(datalake_loaded_at as timestamp) desc
    ) = 1

)

select
    safe_cast(id_gestante as int64) as id_gestante,
    num_cns,
    cpf,
    nme_nome,
    safe_cast(flg_ativo as int64) as flg_ativo,
    safe_cast(created as datetime) as created_at,
    safe_cast(modified as datetime) as updated_at,
    safe_cast(id_user as int64) as id_user,
    safe_cast(id_raca_cor as int64) as id_raca_cor,
    num_cns_video,
    safe_cast(datalake_loaded_at as timestamp) as datalake_loaded_at
from deduplicado