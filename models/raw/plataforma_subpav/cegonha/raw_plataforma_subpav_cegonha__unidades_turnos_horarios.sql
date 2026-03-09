{{ config(
    schema = 'brutos_plataforma_subpav',
    alias = 'unidades_turnos_horarios',
    materialized = 'table'
) }}

with source as (

    select *
    from {{ source('brutos_plataforma_subpav_staging', 'subpav_cegonha__unidades_turnos_horarios') }}

),

base as (

    select
        id_turnos_horario,
        id_turno,
        id_horario,
        id_semana_dia,
        flg_ativo,
        id_unidades_agendamento_vagas,
        created,
        modified,
        datalake_loaded_at
    from source

)

select
    safe_cast(id_turnos_horario as int64) as id_turnos_horario,
    safe_cast(id_turno as int64) as id_turno,
    safe_cast(id_horario as int64) as id_horario,
    safe_cast(id_semana_dia as int64) as id_semana_dia,
    safe_cast(flg_ativo as int64) as flg_ativo,
    safe_cast(id_unidades_agendamento_vagas as int64) as id_unidades_agendamento_vagas,
    safe_cast({{ normalize_null("trim(created)") }} as datetime) as created_at,
    safe_cast({{ normalize_null("trim(modified)") }} as datetime) as updated_at,
    safe_cast(datalake_loaded_at as timestamp) as datalake_loaded_at
from base