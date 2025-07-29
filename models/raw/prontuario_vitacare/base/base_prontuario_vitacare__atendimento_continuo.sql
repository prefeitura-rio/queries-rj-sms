{{
    config(
        schema="brutos_prontuario_vitacare_staging",
        alias="_base_atendimento_continuo",
        materialized="incremental",
        incremental_strategy='merge', 
        unique_key=['id_hci'],
        partition_by={
            "field": "data_particao",
            "data_type": "date",
            "granularity": "month",
        },
        tags=['daily']
    )
}}

{% set partitions_to_replace = (
    "date_sub(current_date('America/Sao_Paulo'), interval 30 day)"
) %}

with

    bruto_atendimento_continuo_com_repeticao as (
        select *,
            source_id  as id_prontuario_local,
            concat(nullif(payload_cnes, ''), '.', nullif(source_id, '')) as id_prontuario_global
        from {{ source("brutos_prontuario_vitacare_staging", "atendimento_continuo") }}
        {% if is_incremental() %} where data_particao >= {{ partitions_to_replace }} {% endif %}
    ),
    bruto_atendimento_eventos_ranqueados as (
        select *,
        from bruto_atendimento_continuo_com_repeticao
        qualify
            row_number() over (partition by id_prontuario_global order by datalake_loaded_at desc) = 1
    ),

    atendimento_continuo as (
        select
            -- PK
            id_prontuario_local,
            id_prontuario_global,

            -- Chaves
            patient_cpf as cpf_paciente,
            payload_cnes as cnes_unidade,

            -- Profissional
            json_extract_scalar(data, "$.profissional.cns") as cns_profissional,
            json_extract_scalar(data, "$.profissional.cpf") as cpf_profissional,
            json_extract_scalar(data, "$.profissional.nome") as nome_profissional,
            json_extract_scalar(data, "$.profissional.cbo") as cbo_profissional,
            json_extract_scalar(data, "$.profissional.cbo_descricao") as cbo_descricao_profissional,

            -- Equipe
            json_extract_scalar(data, "$.profissional.equipe.cod_equipe") as cod_equipe_profissional,
            json_extract_scalar(data, "$.profissional.equipe.cod_ine") as cod_ine_equipe_profissional,
            json_extract_scalar(data, "$.profissional.equipe.nome") as nome_equipe_profissional,

            -- Dados da Consulta
            json_extract_scalar(data, "$.tipo_consulta") as tipo,
            json_extract_scalar(data, "$.eh_coleta") as eh_coleta,
            safe_cast(
                {{ process_null("json_extract_scalar(data, '$.datahora_marcacao_atendimento')") }} as datetime
            ) as datahora_marcacao,
            safe_cast(
                {{ process_null("json_extract_scalar(data, '$.datahora_inicio_atendimento')") }} as datetime
            ) as datahora_inicio,
            safe_cast(
                {{ process_null("json_extract_scalar(data, '$.datahora_fim_atendimento')") }} as datetime
            ) as datahora_fim,

            -- Campos Livres
            json_extract_scalar(data, "$.soap_subjetivo_motivo") as soap_subjetivo_motivo,
            json_extract_scalar(data, "$.soap_objetivo_descricao") as soap_objetivo_descricao,
            json_extract_scalar(data, "$.soap_avaliacao_observacoes") as soap_avaliacao_observacoes,
            json_extract_scalar(data, "$.soap_plano_procedimentos_clinicos") as soap_plano_procedimentos_clinicos,
            json_extract_scalar(data, "$.soap_plano_observacoes") as soap_plano_observacoes,
            json_extract_scalar(data, "$.notas_observacoes") as notas_observacoes,

            -- JSONs
            json_extract(data, "$.prescricoes") as prescricoes,
            json_extract(data, "$.condicoes") as condicoes,
            json_extract(data, "$.exames_solicitados") as exames_solicitados,
            json_extract(data, "$.alergias_anamnese") as alergias_anamnese,
            json_extract(data, "$.vacinas") as vacinas,
            json_extract(data, "$.indicadores") as indicadores,
            json_extract(data, "$.encaminhamentos") as encaminhamentos,

            -- Metadados
            safe_cast(source_updated_at as datetime) as updated_at,
            safe_cast(datalake_loaded_at as datetime) as loaded_at,
            safe_cast(
                safe_cast(json_extract_scalar(data, "$.datahora_fim_atendimento")as datetime)
                as date) as data_particao,

        from bruto_atendimento_eventos_ranqueados
    )
select id_prontuario_local,
            id_prontuario_global,
            {{
                dbt_utils.generate_surrogate_key(
                    [
                        "id_prontuario_global",
                    ]
                )
            }} as id_hci,
            * except (id_prontuario_local,id_prontuario_global),
from atendimento_continuo
