{{
    config(
        alias="transmissao",
    )
}}

with

source as (
    select * from {{ source("brutos_prontuario_vitacare_staging", "transmissao_rnds") }}
),

padronizados as (
    select PERFORMED_VACCINE_ACTION_ID, RIA_RNDS_ID, LOCAL_UUID, ENTID_ID, DATA_ADMINISTRACAO, SEND_STATUS, DATE_SEND_STATUS
    from source
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
    from padronizados
),

deduplicated as (
    select *
    from final
    qualify row_number() over (partition by id_vacinacao_global order by data_envio asc, data_envio desc) = 1
)

select * from deduplicated