{{
    config(
        alias="transmissao",
    )
}}

with

source as (
    select * from {{ source("brutos_prontuario_vitacare_staging", "transmissao_rnds") }}
),

final as (
    select
        concat(lpad(cast(ENTID_ID as string), 7, '0'), '.', cast(PERFORMED_VACCINE_ACTION_ID as string)) as id_vacinacao_global,
        cast(PERFORMED_VACCINE_ACTION_ID as string) as id_vacinacao_local,
        lpad(cast(ENTID_ID as string), 7, '0') as id_cnes,
        RIA_RNDS_ID as uuid_rnds,
        LOCAL_UUID as uuid_local,
        lpad(cast(CPF_UTENTE as string), 11, '0') as cpf_paciente,
        lpad(cast(CNS_UTENTE as string), 15, '0') as cns_paciente,
        lpad(cast(PROFISSIONAL_CNS as string), 15, '0') as cns_profissional,
        SEND_STATUS as status_envio,
        safe_cast(DATE_SEND_STATUS as date) as data_envio,
        SERVER_CODE_RESPONSE as codigo_resposta_servidor,
        SERVER_RESPONSE as resposta_servidor,
        DATE_PROC_REFERENCE as data_referencia_processamento
    from source
),

deduplicated as (
    select *
    from final
    qualify row_number() over (partition by id_vacinacao_global order by data_envio desc) = 1
)

select * from final