{{ config(
    schema = 'brutos_plataforma_subpav',
    alias = 'unidades_agendamento_vagas',
    materialized = 'table',
    meta={"owner": "karen"}
) }}

with source as (

    select *
    from {{ source('brutos_plataforma_subpav_staging', 'subpav_cegonha__unidades_agendamento_vagas') }}

),

base as (

    select
        id_unidades_agendamento_vagas,
        num_total_vagas_turno,
        id_unidades_referencia_encaminha,
        flg_ativo,
        created,
        modified,
        datalake_loaded_at
    from source

),

dados_limpos as (

    select
        {{ normalize_null("trim(id_unidades_agendamento_vagas)") }} as id_unidades_agendamento_vagas,
        {{ normalize_null("trim(num_total_vagas_turno)") }} as num_total_vagas_turno,
        {{ normalize_null("trim(id_unidades_referencia_encaminha)") }} as id_unidades_referencia_encaminha,
        {{ normalize_null("trim(flg_ativo)") }} as flg_ativo,
        {{ normalize_null("trim(created)") }} as created,
        {{ normalize_null("trim(modified)") }} as modified,
        {{ normalize_null("trim(datalake_loaded_at)") }} as datalake_loaded_at
    from base

),

deduplicado as (

    select *
    from dados_limpos
    qualify row_number() over (
        partition by
            id_unidades_agendamento_vagas,
            num_total_vagas_turno,
            id_unidades_referencia_encaminha,
            flg_ativo,
            created,
            modified
        order by safe_cast(datalake_loaded_at as timestamp) desc
    ) = 1

)

select
    safe_cast(id_unidades_agendamento_vagas as int64) as id_unidades_agendamento_vagas,
    safe_cast(num_total_vagas_turno as int64) as num_total_vagas_turno,
    safe_cast(id_unidades_referencia_encaminha as int64) as id_unidades_referencia_encaminha,
    safe_cast(flg_ativo as int64) as flg_ativo,
    safe_cast(created as datetime) as created_at,
    safe_cast(modified as datetime) as updated_at,
    safe_cast(datalake_loaded_at as timestamp) as datalake_loaded_at
from deduplicado