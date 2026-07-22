{{
    config(
        alias="exames",
        materialized="table",
        partition_by={
            "field": "data_particao",
            "data_type": "date",
            "granularity": "day",
        }
    )
}}

with 
    source as (
        select * from {{ source('brutos_medirec_staging', 'cielab_exams_sms') }}
    ),

    exames as (
        select
            {{ process_null("solicitacao_id") }} as solicitacao_id,
            {{ process_null("paciente_nome") }} as paciente_nome,
            {{ process_null("paciente_idade") }} as paciente_idade,
            case 
              when lower({{ process_null("paciente_sexo") }}) = 'masc' then 'Masculino'
              when lower({{ process_null("paciente_sexo") }}) = 'fem' then 'Feminino'
              else null
            end as paciente_sexo,
            datetime(cast({{ process_null("entrada") }} as timestamp)) as entrada,
            datetime(cast({{ process_null("entrega") }} as timestamp)) as entrega,
            datetime(cast({{ process_null("recebido") }} as timestamp)) as recebido,
            
            {{ process_null("mic") }} as mic,
            {{ process_null("st") }} as st,
            {{ process_null("f") }} as f,
            {{ process_null("has_alert") }} as has_alert,
            {{ process_null("cns") }} as cns,
            {{ process_null("exames") }} as exames,
            {{ process_null("alterados") }} as alterados,
            date(cast({{ process_null("data_nasc") }} as datetime)) as data_nascimento,
            {{ process_null("unit_ap") }} as area_programatica,
            {{ process_null("unit_cnes") }} as id_cnes,
            {{ process_null("unit_name") }} as unidade_nome,
            date(cast({{ process_null("entrada") }} as timestamp)) as data_particao

        from source
    )

select * from exames