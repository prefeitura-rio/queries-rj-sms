{{ config(
    schema = 'brutos_plataforma_subpav',
    alias = 'sisare__internacoes',
    materialized = 'table'
) }}

with source as (

    select *
    from {{ source('brutos_plataforma_subpav_staging', 'subpav_altas_referenciadas__internacoes') }}

),

base as (

    select
        id_internacao,
        id_paciente,
        num_prontuario,
        unidade_referencia,
        unidade_atendimento,
        dt_entrada,
        dt_saida,
        id_motivo_internacao,
        motivo_internacao,
        id_forma_entrada,
        status,
        created_at,
        updated_at,
        peso,
        id_apgar_1,
        id_apgar_5,
        equipe_referencia,
        datalake_loaded_at
    from source

),

dados_limpos as (

    select
        {{ normalize_null("trim(id_internacao)") }} as id_internacao,
        {{ normalize_null("trim(id_paciente)") }} as id_paciente,
        {{ normalize_null("trim(num_prontuario)") }} as num_prontuario,
        {{ normalize_null("regexp_replace(trim(unidade_referencia), r'\\.0$', '')") }} as unidade_referencia,
        {{ normalize_null("trim(unidade_atendimento)") }} as unidade_atendimento,
        {{ normalize_null("trim(dt_entrada)") }} as dt_entrada,
        {{ normalize_null("trim(dt_saida)") }} as dt_saida,
        {{ normalize_null("regexp_replace(trim(id_motivo_internacao), r'\\.0$', '')") }} as id_motivo_internacao,
        {{ normalize_null("trim(motivo_internacao)") }} as motivo_internacao,
        {{ normalize_null("regexp_replace(trim(id_forma_entrada), r'\\.0$', '')") }} as id_forma_entrada,
        {{ normalize_null("trim(status)") }} as status,
        {{ normalize_null("trim(created_at)") }} as created_at,
        {{ normalize_null("trim(updated_at)") }} as updated_at,
        {{ normalize_null("trim(peso)") }} as peso,
        {{ normalize_null("regexp_replace(trim(id_apgar_1), r'\\.0$', '')") }} as id_apgar_1,
        {{ normalize_null("regexp_replace(trim(id_apgar_5), r'\\.0$', '')") }} as id_apgar_5,
        {{ normalize_null("regexp_replace(trim(equipe_referencia), r'\\.0$', '')") }} as equipe_referencia,
        {{ normalize_null("trim(datalake_loaded_at)") }} as datalake_loaded_at
    from base

),

deduplicado as (

    select *
    from dados_limpos
    qualify row_number() over (
        partition by
            id_internacao,
            id_paciente,
            num_prontuario,
            unidade_referencia,
            unidade_atendimento,
            dt_entrada,
            dt_saida,
            id_motivo_internacao,
            motivo_internacao,
            id_forma_entrada,
            status,
            created_at,
            updated_at,
            peso,
            id_apgar_1,
            id_apgar_5,
            equipe_referencia
        order by safe_cast(datalake_loaded_at as timestamp) desc
    ) = 1

)

select
    id_internacao,
    id_paciente,
    num_prontuario,
    safe_cast(unidade_referencia as int64) as unidade_referencia,
    safe_cast(unidade_atendimento as int64) as unidade_atendimento,
    safe.parse_date('%Y-%m-%d', dt_entrada) as dt_entrada,
    safe.parse_date('%Y-%m-%d', dt_saida) as dt_saida,
    safe_cast(id_motivo_internacao as int64) as id_motivo_internacao,
    motivo_internacao,
    safe_cast(id_forma_entrada as int64) as id_forma_entrada,
    safe_cast(status as int64) as status,
    safe.parse_datetime('%Y-%m-%d %H:%M:%S', created_at) as created_at,
    safe.parse_datetime('%Y-%m-%d %H:%M:%S', updated_at) as updated_at,
    safe_cast(peso as int64) as peso,
    safe_cast(id_apgar_1 as int64) as id_apgar_1,
    safe_cast(id_apgar_5 as int64) as id_apgar_5,
    safe_cast(equipe_referencia as int64) as equipe_referencia,
    safe_cast(datalake_loaded_at as timestamp) as datalake_loaded_at
from deduplicado