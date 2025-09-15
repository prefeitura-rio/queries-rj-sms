{{
    config(
        alias="exames",
        schema="brutos_laudos_medilab",
        materialized="table",
        partition_by={
            "field": "data_particao",
            "data_type": "date",
            "granularity": "day",
        }
    )
}}

with source as (
    select * from {{ source('brutos_laudos_medilab_staging', 'laudo_continuo') }}
),

exams as (
    select
        {{ process_null("json_extract_scalar(data, '$.numReport')") }} as id_laudo,
        {{ process_null("json_extract_scalar(data, '$.numExam')") }} as id_exame,
        {{ process_null("json_extract_scalar(data, '$.dateTimeReport')") }} as laudo_data_atualizacao,
        {{ process_null("json_extract_scalar(data, '$.doctorNameRequesting')") }} as medico_requisitante,
        {{ process_null("json_extract_scalar(data, '$.doctorNameReport')") }} as medico_responsavel,
        {{ process_null("json_extract_scalar(data, '$.doctorNameReviewing')") }} as medico_revisor,
        {{ process_null("json_extract_scalar(data, '$.patientCpf')") }} as paciente_cpf,
        {{ process_null("json_extract_scalar(data, '$.patientCns')") }} as paciente_cns,
        {{ process_null("json_extract_scalar(data, '$.patientName')") }} as paciente_nome,
        {{ process_null("json_extract_scalar(data, '$.patientMother')") }} as paciente_mae_nome,
        {{ process_null("json_extract_scalar(data, '$.patientDateOfBirth')") }} as paciente_data_nascimento,
        {{ process_null("json_extract_scalar(data, '$.cnes')") }} as id_cnes,
        {{ process_null("json_extract_scalar(data, '$.dateTimeExam')") }} as exame_data,
        {{ process_null("json_extract_scalar(data, '$.examName')") }} as exame_nome,
        {{ process_null("json_extract_scalar(data, '$.codExamSigtap')") }} as exame_codigo_sigtap,
        safe_cast(safe_cast({{ process_null("json_extract_scalar(data, '$.dateTimeExam')") }} as datetime) as date) as data_particao
    from source
)

select * from exams