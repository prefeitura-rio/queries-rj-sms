{{
    config(
        alias="transmissao",
    )
}}

with

source as (
    select * from {{ source("brutos_prontuario_vitacare_staging", "transmissao_rnds") }}
),

casos_problematicos_concatenados as (
    select * from source
    where REGEXP_CONTAINS(PERFORMED_VACCINE_ACTION_ID, r',')
),

casos_problematicos_shift as (
    select * from source
    where SEND_STATUS not in ('FATAL_ERROR', 'SENT', 'NOT_SENT', 'PENDING_RETRY')
),

casos_padrao as (
    select * from source
    where 
        not (
            REGEXP_CONTAINS(PERFORMED_VACCINE_ACTION_ID, r',') or 
            SEND_STATUS not in ('FATAL_ERROR', 'SENT', 'NOT_SENT', 'PENDING_RETRY')
        )
),

-- ----------------------------
-- Resolvendo Casos Problematicos
-- ----------------------------
casos_problematicos_concatenados_resolvidos as (
    select
        split(PERFORMED_VACCINE_ACTION_ID, ',')[safe_offset(0)] as PERFORMED_VACCINE_ACTION_ID,
        split(PERFORMED_VACCINE_ACTION_ID, ',')[safe_offset(1)] as RIA_RNDS_ID,
        split(PERFORMED_VACCINE_ACTION_ID, ',')[safe_offset(2)] as LOCAL_UUID,
        split(PERFORMED_VACCINE_ACTION_ID, ',')[safe_offset(5)] as ENTID_ID,
        split(PERFORMED_VACCINE_ACTION_ID, ',')[safe_offset(7)] as DATA_ADMINISTRACAO,
        split(PERFORMED_VACCINE_ACTION_ID, ',')[safe_offset(8)] as SEND_STATUS,
        split(PERFORMED_VACCINE_ACTION_ID, ',')[safe_offset(9)] as DATE_SEND_STATUS,
        split(PERFORMED_VACCINE_ACTION_ID, ',')[safe_offset(10)] as SERVER_CODE_RESPONSE
    from casos_problematicos_concatenados
),

casos_problematicos_shift_resolvidos as (
    select 
        PERFORMED_VACCINE_ACTION_ID,
        RIA_RNDS_ID,
        LOCAL_UUID,
        ENTID_ID,
        SEND_STATUS as DATA_ADMINISTRACAO,
        DATE_SEND_STATUS as SEND_STATUS,
        SERVER_CODE_RESPONSE as DATE_SEND_STATUS,
        SERVER_RESPONSE as SERVER_CODE_RESPONSE
    from casos_problematicos_shift
),

padronizados as (
    select PERFORMED_VACCINE_ACTION_ID, RIA_RNDS_ID, LOCAL_UUID, ENTID_ID, DATA_ADMINISTRACAO, SEND_STATUS, DATE_SEND_STATUS, SERVER_CODE_RESPONSE
    from casos_problematicos_concatenados_resolvidos

    union all

    select PERFORMED_VACCINE_ACTION_ID, RIA_RNDS_ID, LOCAL_UUID, ENTID_ID, DATA_ADMINISTRACAO, SEND_STATUS, DATE_SEND_STATUS, SERVER_CODE_RESPONSE
    from casos_problematicos_shift_resolvidos

    union all

    select PERFORMED_VACCINE_ACTION_ID, RIA_RNDS_ID, LOCAL_UUID, ENTID_ID, DATA_ADMINISTRACAO, SEND_STATUS, DATE_SEND_STATUS, SERVER_CODE_RESPONSE
    from casos_padrao
),

final as (
    select
        concat(lpad(cast({{process_null('ENTID_ID')}} as string), 7, '0'), '.', cast({{process_null('PERFORMED_VACCINE_ACTION_ID')}} as string)) as id_vacinacao_global,
        cast({{process_null('PERFORMED_VACCINE_ACTION_ID')}} as string) as id_vacinacao_local,
        lpad(cast({{process_null('ENTID_ID')}} as string), 7, '0') as id_cnes,
        {{process_null('RIA_RNDS_ID')}} as uuid_rnds,
        {{process_null('DATA_ADMINISTRACAO')}} as data_aplicacao,
        {{process_null('LOCAL_UUID')}} as uuid_local,
        {{process_null('SEND_STATUS')}} as status_envio,
        safe_cast({{process_null('DATE_SEND_STATUS')}} as date) as data_envio,
        {{process_null('SERVER_CODE_RESPONSE')}} as codigo_resposta_servidor,
    from padronizados
),

deduplicated as (
    select *
    from final
    qualify row_number() over (partition by id_vacinacao_global order by codigo_resposta_servidor asc, data_envio desc) = 1
)

select * from deduplicated