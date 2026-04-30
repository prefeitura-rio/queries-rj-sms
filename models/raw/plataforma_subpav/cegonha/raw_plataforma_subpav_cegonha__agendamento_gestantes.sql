{{ config(
    schema = 'brutos_plataforma_subpav',
    alias = 'agendamento_gestantes',
    materialized = 'table',
    meta={"owner": "karen"}
) }}

with source as (

    select *
    from {{ source('brutos_plataforma_subpav_staging', 'subpav_cegonha__agendamento_gestantes') }}

),

base as (

    select
        id_agendamento_gestante,
        dta_provavel_parto,
        dta_visita_maternidade,
        txt_obs,
        created,
        modified,
        id_gestante,
        id_turnos_horarios,
        flg_remarcacao,
        id_user,
        idade_gestacional,
        id_agendamento_gestante_remarcacao,
        id_gestacao_tipo,
        flg_ativo,
        situacao_rua,
        tel_contato,
        nome_acompanhante,
        tel_contato_acompanhante,
        datalake_loaded_at
    from source

),

dados_limpos as (

    select
        {{ normalize_null("trim(id_agendamento_gestante)") }} as id_agendamento_gestante,
        {{ normalize_null("trim(dta_provavel_parto)") }} as dta_provavel_parto,
        {{ normalize_null("trim(dta_visita_maternidade)") }} as dta_visita_maternidade,

        {{ normalize_null(
            "trim(regexp_replace(regexp_replace(txt_obs, r'[\\n\\r\\t]+', ' '), r'\\s+', ' '))"
        ) }} as txt_obs,

        {{ normalize_null("trim(created)") }} as created,
        {{ normalize_null("trim(modified)") }} as modified,
        {{ normalize_null("trim(id_gestante)") }} as id_gestante,
        {{ normalize_null("regexp_replace(trim(id_turnos_horarios), r'\\.0$', '')") }} as id_turnos_horarios,
        {{ normalize_null("regexp_replace(trim(flg_remarcacao), r'\\.0$', '')") }} as flg_remarcacao,
        {{ normalize_null("trim(id_user)") }} as id_user,
        {{ normalize_null("trim(idade_gestacional)") }} as idade_gestacional,
        {{ normalize_null("regexp_replace(trim(id_agendamento_gestante_remarcacao), r'\\.0$', '')") }} as id_agendamento_gestante_remarcacao,
        {{ normalize_null("regexp_replace(trim(id_gestacao_tipo), r'\\.0$', '')") }} as id_gestacao_tipo,
        {{ normalize_null("regexp_replace(trim(flg_ativo), r'\\.0$', '')") }} as flg_ativo,
        {{ normalize_null("regexp_replace(trim(situacao_rua), r'\\.0$', '')") }} as situacao_rua,
        {{ normalize_null("trim(tel_contato)") }} as tel_contato,

        {{ normalize_null(
            "trim(regexp_replace(regexp_replace(nome_acompanhante, r'[\\n\\r\\t]+', ' '), r'\\s+', ' '))"
        ) }} as nome_acompanhante,

        {{ normalize_null("trim(tel_contato_acompanhante)") }} as tel_contato_acompanhante,
        {{ normalize_null("trim(datalake_loaded_at)") }} as datalake_loaded_at
    from base

),

deduplicado as (

    select *
    from dados_limpos
    qualify row_number() over (
        partition by
            id_agendamento_gestante,
            dta_provavel_parto,
            dta_visita_maternidade,
            txt_obs,
            created,
            modified,
            id_gestante,
            id_turnos_horarios,
            flg_remarcacao,
            id_user,
            idade_gestacional,
            id_agendamento_gestante_remarcacao,
            id_gestacao_tipo,
            flg_ativo,
            situacao_rua,
            tel_contato,
            nome_acompanhante,
            tel_contato_acompanhante
        order by safe_cast(datalake_loaded_at as timestamp) desc
    ) = 1

)

select
    safe_cast(id_agendamento_gestante as int64) as id_agendamento_gestante,
    safe_cast(dta_provavel_parto as date) as dta_provavel_parto,
    safe_cast(dta_visita_maternidade as date) as dta_visita_maternidade,
    txt_obs,
    safe_cast(created as datetime) as created_at,
    safe_cast(modified as datetime) as updated_at,
    safe_cast(id_gestante as int64) as id_gestante,
    safe_cast(id_turnos_horarios as int64) as id_turnos_horarios,
    safe_cast(flg_remarcacao as int64) as flg_remarcacao,
    safe_cast(id_user as int64) as id_user,
    safe_cast(idade_gestacional as int64) as idade_gestacional,
    safe_cast(id_agendamento_gestante_remarcacao as int64) as id_agendamento_gestante_remarcacao,
    safe_cast(id_gestacao_tipo as int64) as id_gestacao_tipo,
    safe_cast(flg_ativo as int64) as flg_ativo,
    safe_cast(situacao_rua as int64) as situacao_rua,
    tel_contato,
    nome_acompanhante,
    tel_contato_acompanhante,
    safe_cast(datalake_loaded_at as timestamp) as datalake_loaded_at
from deduplicado