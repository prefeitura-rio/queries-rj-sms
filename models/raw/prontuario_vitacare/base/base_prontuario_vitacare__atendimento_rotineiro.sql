{{
    config(
        alias="_base_atendimento_rotineiro",
        materialized="table",
    )
}}

with
    bruto_atendimento_eventos_com_repeticao as (
        select 
            *,
            concat(nullif(payload_cnes, ''), '.', nullif(source_id, '')) as gid
        from {{ source("brutos_prontuario_vitacare_staging", "atendimento_eventos") }} 
    ),
    bruto_atendimento_eventos_ranqueados as (
        select
            *,
            row_number() over (partition by gid order by datalake_loaded_at desc) as rank
        from bruto_atendimento_eventos_com_repeticao
    )
select
    -- PK
    safe_cast(gid as string) as gid,

    -- Chaves
    safe_cast(patient_cpf as string) as cpf,
    safe_cast(payload_cnes as string) as cnes_unidade,

    -- Profissional
    safe_cast(data__profissional__cns as string) as cns_profissional,
    safe_cast(data__profissional__cpf as string) as cpf_profissional,
    safe_cast(data__profissional__nome as string) as nome_profissional,
    safe_cast(data__profissional__cbo as string) as cbo_profissional,
    safe_cast(data__profissional__cbo_descricao as string) as cbo_descricao_profissional,
    safe_cast(data__profissional__equipe__cod_equipe as string) as cod_equipe_profissional,
    safe_cast(data__profissional__equipe__cod_ine as string) as cod_ine_equipe_profissional,
    safe_cast(data__profissional__equipe__nome as string) as nome_equipe_profissional,

    -- Dados da Consulta
    safe_cast(data__tipo_consulta as string) as tipo,
    safe_cast(data__eh_coleta as string) as eh_coleta,
    safe_cast({{ process_null('data__datahora_marcacao_atendimento') }} as datetime) as datahora_marcacao,
    safe_cast({{ process_null('data__datahora_inicio_atendimento') }} as datetime) as datahora_inicio,
    safe_cast({{ process_null('data__datahora_fim_atendimento') }} as datetime) as datahora_fim,

    -- Campos Livres
    safe_cast({{ process_null('data__soap_subjetivo_motivo') }} as string) as soap_subjetivo_motivo,
    safe_cast({{ process_null('data__soap_objetivo_descricao') }} as string) as soap_objetivo_descricao,
    safe_cast({{ process_null('data__soap_avaliacao_observacoes') }} as string) as soap_avaliacao_observacoes,
    safe_cast({{ process_null('data__soap_plano_procedimentos_clinicos') }} as string) as soap_plano_procedimentos_clinicos,
    safe_cast({{ process_null('data__soap_plano_observacoes') }} as string) as soap_plano_observacoes,
    safe_cast({{ process_null('data__notas_observacoes') }} as string) as soap_notas_observacoes,

    -- JSONs
    safe_cast({{ dict_to_json('data__prescricoes') }} as string) as prescricoes,
    safe_cast({{ dict_to_json('data__condicoes') }} as string) as condicoes,
    safe_cast({{ dict_to_json('data__exames_solicitados') }} as string) as exames_solicitados,
    safe_cast({{ dict_to_json('data__alergias_anamnese') }} as string) as alergias_anamnese,
    safe_cast({{ dict_to_json('data__vacinas') }} as string) as vacinas,
    safe_cast({{ dict_to_json('data__indicadores') }} as string) as indicadores,
    safe_cast({{ dict_to_json('data__encaminhamentos') }} as string) as encaminhamentos,

    -- Metadados
    safe_cast(source_updated_at as datetime) as updated_at,
    safe_cast(datalake_loaded_at as datetime) as loaded_at,
    safe_cast({{ process_null('data__datahora_inicio_atendimento') }} as date) as data_particao,

from bruto_atendimento_eventos_ranqueados 
where rank = 1