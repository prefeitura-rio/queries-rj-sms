{{ config(
    schema = 'brutos_plataforma_subpav',
    alias = 'sisare__vw_altas',
    materialized = 'table'
) }}

with source as (

    select *
    from {{ source('brutos_plataforma_subpav_staging', 'subpav_altas_referenciadas__vw_altas') }}

),

base as (

    select
        id_alta,
        id_paciente,
        id_internacao,
        id_formulario,
        id_motivo_alta,
        local_transferencia,
        motivo_encaminhamento,
        dt_obito,
        causa_obito,
        unidade_aps,
        resumo_recomendacoes,
        conduta_indicada,
        gestacao_em_curso,
        status,
        created_at,
        updated_at,
        urgente,
        id_user,
        alta_pendente,
        cpf_cadastrante,
        datalake_loaded_at
    from source

),

dados_limpos as (

    select
        {{ normalize_null("trim(id_alta)") }} as id_alta,
        {{ normalize_null("trim(id_paciente)") }} as id_paciente,
        {{ normalize_null("trim(id_internacao)") }} as id_internacao,
        {{ normalize_null("regexp_replace(trim(id_formulario), r'\\.0$', '')") }} as id_formulario,
        {{ normalize_null("regexp_replace(trim(id_motivo_alta), r'\\.0$', '')") }} as id_motivo_alta,
        {{ normalize_null("trim(local_transferencia)") }} as local_transferencia,
        {{ normalize_null("trim(motivo_encaminhamento)") }} as motivo_encaminhamento,
        {{ normalize_null("trim(dt_obito)") }} as dt_obito,
        {{ normalize_null("trim(causa_obito)") }} as causa_obito,
        {{ normalize_null("regexp_replace(trim(unidade_aps), r'\\.0$', '')") }} as unidade_aps,
        {{ normalize_null("trim(resumo_recomendacoes)") }} as resumo_recomendacoes,
        {{ normalize_null("trim(conduta_indicada)") }} as conduta_indicada,
        {{ normalize_null("trim(gestacao_em_curso)") }} as gestacao_em_curso,
        {{ normalize_null("trim(status)") }} as status,
        {{ normalize_null("trim(created_at)") }} as created_at,
        {{ normalize_null("trim(updated_at)") }} as updated_at,
        {{ normalize_null("trim(urgente)") }} as urgente,
        {{ normalize_null("trim(id_user)") }} as id_user,
        {{ normalize_null("trim(alta_pendente)") }} as alta_pendente,
        {{ normalize_null("trim(cpf_cadastrante)") }} as cpf_cadastrante,
        {{ normalize_null("trim(datalake_loaded_at)") }} as datalake_loaded_at
    from base

),

deduplicado as (

    select *
    from dados_limpos
    qualify row_number() over (
        partition by id_alta
        order by
            safe_cast(updated_at as timestamp) desc nulls last,
            safe_cast(created_at as timestamp) desc nulls last,
            safe_cast(datalake_loaded_at as timestamp) desc
    ) = 1

)

select
    safe_cast(id_alta as int64) as id_alta,
    safe_cast(id_paciente as int64) as id_paciente,
    safe_cast(id_internacao as int64) as id_internacao,
    safe_cast(id_formulario as int64) as id_formulario,
    safe_cast(id_motivo_alta as int64) as id_motivo_alta,
    local_transferencia,
    motivo_encaminhamento,
    safe_cast(dt_obito as date) as dt_obito,
    causa_obito,
    safe_cast(unidade_aps as int64) as unidade_aps,
    resumo_recomendacoes,
    conduta_indicada,
    gestacao_em_curso,
    safe_cast(status as int64) as status,
    safe_cast(created_at as timestamp) as created_at,
    safe_cast(updated_at as timestamp) as updated_at,
    safe_cast(urgente as int64) as urgente,
    safe_cast(id_user as int64) as id_user,
    safe_cast(alta_pendente as int64) as alta_pendente,
    cpf_cadastrante,
    safe_cast(datalake_loaded_at as timestamp) as datalake_loaded_at
from deduplicado
