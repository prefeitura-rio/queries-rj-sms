{{
    config(
        alias="exames",
        materialized="table",
        partition_by={
            "field": "exame_data_particao",
            "data_type": "date",
            "granularity": "day",
        }
    )
}}

with 
    source as (
        select * from {{ source('brutos_laudos_medilab_staging', 'laudo_continuo') }}
    ),

    exams as (
        select
            {{ process_null("json_extract_scalar(data, '$.numReport')") }} as id_laudo,
            case
                when {{ process_null("json_extract_scalar(data, '$.reportPDF')") }} like 'gs://%'
                then {{ process_null("json_extract_scalar(data, '$.reportPDF')") }}
                else null
            end as laudo_bucket,

            {{ process_null("json_extract_scalar(data, '$.reportText')") }} as laudo_texto,

            {{ process_null("json_extract_scalar(data, '$.numExam')") }} as id_exame,

            safe_cast(safe_cast({{ process_null("json_extract_scalar(data, '$.dateTimeReport')") }} as timestamp) as datetime) as laudo_data_atualizacao,
            
            case
                when lower({{ process_null("trim(json_extract_scalar(data, '$.doctorNameRequesting'))") }}) = 'ilegivel'  then null
                when lower({{ process_null("trim(json_extract_scalar(data, '$.doctorNameRequesting'))") }}) = 'medico ilegivel'  then null
                when lower({{ process_null("trim(json_extract_scalar(data, '$.doctorNameRequesting'))") }}) = 'medico interno'  then null
                else ({{ process_null("trim(json_extract_scalar(data, '$.doctorNameRequesting'))") }})
            end as medico_requisitante,
            {{ process_null("trim(json_extract_scalar(data, '$.doctorNameReport'))") }} as medico_responsavel,
            {{ process_null("trim(json_extract_scalar(data, '$.doctorNameReviewing'))") }} as medico_revisor,
            {{ process_null("json_extract_scalar(data, '$.patientCpf')") }} as paciente_cpf,
            {{ process_null("json_extract_scalar(data, '$.patientCns')") }} as paciente_cns,
            {{ process_null("trim(json_extract_scalar(data, '$.patientName'))") }} as paciente_nome,
            {{ process_null("trim(json_extract_scalar(data, '$.patientMother'))") }} as paciente_mae_nome,
            safe_cast(safe_cast({{ process_null("json_extract_scalar(data, '$.patientDateOfBirth')") }} as timestamp) as date) as paciente_data_nascimento,
            {{ process_null("json_extract_scalar(data, '$.cnes')") }} as id_cnes,
            safe_cast(safe_cast({{ process_null("json_extract_scalar(data, '$.dateTimeExam')") }} as timestamp) as date) as exame_data,
            {{ process_null("json_extract_scalar(data, '$.examName')") }} as exame_nome,
            {{ process_null("json_extract_scalar(data, '$.codExamSigtap')") }} as exame_codigo_sigtap,
            safe_cast(safe_cast({{ process_null("json_extract_scalar(data, '$.dateTimeExam')") }} as timestamp) as date) as exame_data_particao
        from source
    ),

    dedup_exames as (
        select 
            * 
        from exams
        qualify row_number() over(partition by id_exame, id_laudo order by laudo_data_atualizacao desc) = 1
    )

select * from dedup_exames