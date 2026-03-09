{{ config(
    schema = 'brutos_plataforma_subpav',
    alias = 'unidades_agendamento_vagas',
    materialized = 'table'
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

)

select
    safe_cast(id_unidades_agendamento_vagas as int64) as id_unidades_agendamento_vagas,
    safe_cast(num_total_vagas_turno as int64) as num_total_vagas_turno,
    safe_cast(id_unidades_referencia_encaminha as int64) as id_unidades_referencia_encaminha,
    safe_cast(flg_ativo as int64) as flg_ativo,
    safe_cast({{ normalize_null("trim(created)") }} as datetime) as created_at,
    safe_cast({{ normalize_null("trim(modified)") }} as datetime) as updated_at,
    safe_cast(datalake_loaded_at as timestamp) as datalake_loaded_at
from base