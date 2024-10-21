{{
    config(
        schema="brutos_prontuario_vitacare_staging",
        alias="_base_atendimento_rotineiro",
        materialized="incremental",
        incremental_strategy="insert_overwrite",
        partition_by={
            "field": "data_particao",
            "data_type": "date",
            "granularity": "month",
        },
    )
}}

{% set partitions_to_replace = (
    "date_sub(current_date('America/Sao_Paulo'), interval 30 day)"
) %}

with
    bruto_atendimento_eventos_com_repeticao as (
        select *, concat(nullif(payload_cnes, ''), '.', nullif(source_id, '')) as gid
        from {{ source("brutos_prontuario_vitacare_staging", "atendimento_eventos") }}
    ),
    bruto_atendimento_eventos_ranqueados as (
        select *,

        from bruto_atendimento_eventos_com_repeticao
        qualify
            row_number() over (partition by gid order by datalake_loaded_at desc) = 1
    ),

    final as (
        select
            -- PK
            gid as gid,

            -- Chaves
            patient_cpf as cpf,
            payload_cnes as cnes_unidade,

            -- Profissional
            data__profissional__cns as cns_profissional,
            data__profissional__cpf as cpf_profissional,
            data__profissional__nome as nome_profissional,
            data__profissional__cbo as cbo_profissional,
            data__profissional__cbo_descricao as cbo_descricao_profissional,
            data__profissional__equipe__cod_equipe as cod_equipe_profissional,
            data__profissional__equipe__cod_ine as cod_ine_equipe_profissional,
            data__profissional__equipe__nome as nome_equipe_profissional,

            -- Dados da Consulta
            data__tipo_consulta as tipo,
            data__eh_coleta as eh_coleta,
            safe_cast(
                {{ process_null("data__datahora_marcacao_atendimento") }} as datetime
            ) as datahora_marcacao,
            safe_cast(
                {{ process_null("data__datahora_inicio_atendimento") }} as datetime
            ) as datahora_inicio,
            safe_cast(
                {{ process_null("data__datahora_fim_atendimento") }} as datetime
            ) as datahora_fim,

            -- Campos Livres
            {{ process_null("data__soap_subjetivo_motivo") }} as soap_subjetivo_motivo,
            {{ process_null("data__soap_objetivo_descricao") }} as soap_objetivo_descricao,
            {{ process_null("data__soap_avaliacao_observacoes") }} as soap_avaliacao_observacoes,
            {{ process_null("data__soap_plano_procedimentos_clinicos") }} as soap_plano_procedimentos_clinicos,
            {{ process_null("data__soap_plano_observacoes") }} as soap_plano_observacoes,
            {{ process_null("data__notas_observacoes") }} as soap_notas_observacoes,

            -- JSONs
            data__prescricoes as prescricoes,
            data__condicoes as condicoes,
            data__exames_solicitados as exames_solicitados,
            data__alergias_anamnese as alergias_anamnese,
            data__vacinas as vacinas,
            data__indicadores as indicadores,
            data__encaminhamentos as encaminhamentos,

            -- Metadados
            safe_cast(source_updated_at as datetime) as updated_at,
            safe_cast(datalake_loaded_at as datetime) as loaded_at,
            safe_cast(
                safe_cast(data__datahora_fim_atendimento as datetime) as date
            ) as data_particao,

        from bruto_atendimento_eventos_ranqueados
    )

select *
from final
{% if is_incremental() %} where data_particao >= {{ partitions_to_replace }} {% endif %}
