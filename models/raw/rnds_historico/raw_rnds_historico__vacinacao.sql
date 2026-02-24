{{
    config(
        alias="vacinacao", 
        materialized="incremental",
        unique_key = 'id_global',
        schema="brutos_rnds_historico",
        partition_by={
            "field": "data_particao",
            "data_type": "date",
            "granularity": "day"
        },
        cluster_by= 'id_global'
    )
}}

with 
    source as (
        select *
        from {{ source('brutos_rnds_historico_staging', 'ap10_rnds_ria_routine_immunization_control') }}
        union all
        select *
        from {{ source('brutos_rnds_historico_staging', 'ap21_rnds_ria_routine_immunization_control') }}
        union all
        select *
        from {{ source('brutos_rnds_historico_staging', 'ap22_rnds_ria_routine_immunization_control') }}
        union all
        select *
        from {{ source('brutos_rnds_historico_staging', 'ap31_rnds_ria_routine_immunization_control') }}
        union all
        select *
        from {{ source('brutos_rnds_historico_staging', 'ap32_rnds_ria_routine_immunization_control') }}
        union all
        select *
        from {{ source('brutos_rnds_historico_staging', 'ap33_rnds_ria_routine_immunization_control') }}
        union all
        select *
        from {{ source('brutos_rnds_historico_staging', 'ap40_rnds_ria_routine_immunization_control') }}
        union all
        select *
        from {{ source('brutos_rnds_historico_staging', 'ap51_rnds_ria_routine_immunization_control') }}
        union all
        select *
        from {{ source('brutos_rnds_historico_staging', 'ap52_rnds_ria_routine_immunization_control') }}
        union all
        select *
        from {{ source('brutos_rnds_historico_staging', 'ap53_rnds_ria_routine_immunization_control') }}
    ),

    cast_rename as (
        select  
            {{ process_null('ENTID_ID') }} as id_cnes,
            {{ process_null('PERFORMED_VACCINE_ACTION_ID') }} as id_vacinacao,
            {{ process_null('LOCAL_UUID') }} as id_global,
            {{ process_null('Id') }} as id_local,
            {{ process_null('RIA_RNDS_ID') }} as id_rnds,

            {{ process_null('CPF_UTENTE') }} as paciente_cpf,
            {{ process_null('CNS_UTENTE') }} as paciente_cns,

            {{ process_null('IMUNOBIOLOGICO_DISPLAY') }} as vacina_codigo_imunobiologico,
            safe_cast({{ process_null('DATA_ADMINISTRACAO') }} as date) as vacina_aplicacao_data,
            {{ process_null('REPORT_ORIGIN_DISPLAY') }} as vacina_tipo_registro,
            {{ process_null('LOCATION_SYSTEM') }} as vacina_local_aplicacao,
            {{ process_null('FABRICANTE_DISPLAY') }} as vacina_fabricante,
            {{ process_null('LOTE') }} as vacina_lote,
            safe_cast({{ process_null('EXPIRATION_DATE') }} as date) as vacina_validade_lote,

            {{ process_null('LOCAL_APLICACAO_CODE') }} as vacina_local_aplicacao_codigo,
            {{ process_null('VIA_ADMINISTRACAO_CODE') }} as vacina_via_administracao_codigo,
            {{ process_null('PROFISSIONAL_CNS') }} as profissional_cns,

            {{ process_null('ESTRATEGIA_VACINACAO_DISPLAY') }} as vacina_estrategia,
            {{ process_null("
            case 
                when DOSE_DISPLAY = 'PRIMEIRA_DOSE' then '1 dose'
                when DOSE_DISPLAY = 'SEGUNDA_DOSE' then '2 dose'
                when DOSE_DISPLAY = 'TERCEIRA_DOSE' then '3 dose'
                when DOSE_DISPLAY = 'QUARTA_DOSE' then '4 dose'
                when DOSE_DISPLAY = 'QUINTA_DOSE' then '5 dose'

                when DOSE_DISPLAY = 'PRIMEIRO_REFORCO' then '1 reforço'
                when DOSE_DISPLAY = 'SEGUNDO_REFORCO' then '2 reforço'
                when DOSE_DISPLAY = 'TERCEIRO_REFORCO' then '3 reforço'
                when DOSE_DISPLAY = 'REFORCO' then 'reforço'

                when DOSE_DISPLAY = 'UNICA' then 'única'
                when DOSE_DISPLAY = 'REVACINACAO' then 'revacinação'
                when DOSE_DISPLAY = 'DOSE' then 'dose'

                else lower(DOSE_DISPLAY)
            end") }} as vacina_dose,

            safe_cast({{ process_null('DATE_PROC_REFERENCE') }} as date) as data_referencia_processamento,
            {{ process_null('SEND_STATUS') }} as status_envio,
            safe_cast({{ process_null('DATE_SEND_STATUS') }} as date) as data_envio,
            {{ process_null('SERVER_CODE_RESPONSE') }} as codigo_resposta_servidor,
            {{ process_null('SERVER_RESPONSE') }} as mensagem_resposta_servidor,

            safe_cast({{ process_null('extracted_at') }} as datetime) as extracted_at,
            {{ process_null('ano_particao') }} as ano_particao,
            {{ process_null('mes_particao') }} as mes_particao,
            safe_cast({{ process_null('data_particao') }} as date) as data_particao,
        from source

    )

    select 
        *
    from cast_rename



