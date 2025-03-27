{{
    config(
        schema="brutos_prontuario_vitacare_staging",
        alias="_base_atendimento_continuo",
        materialized="table",
    )
}}

with

    source as (
        select * from {{ source("brutos_prontuario_vitacare_staging", "atendimento_continuo") }}
    ),

    atendimento_continuo as (
        select
            patient_cpf as cpf_paciente,
            source_id as id_atendimento,
            json_extract(data, "$.unidade_ap") as unidade_ap,
            json_extract(data, "$.unidade_cnes") as unidade_cnes,
            json_extract(data, "$.profissional") as profissional,
            json_extract(data, "$.datahora_inicio_atendimento") as datahora_inicio_atendimento,
            json_extract(data, "$.datahora_fim_atendimento") as datahora_fim_atendimento,
            json_extract(data, "$.datahora_marcacao_atendimento") as datahora_marcacao_atendimento,
            json_extract(data, "$.tipo_consulta") as tipo_consulta,
            json_extract(data, "$.eh_coleta") as eh_coleta,
            json_extract(data, "$.soap_subjetivo_motivo") as soap_subjetivo_motivo,
            json_extract(data, "$.soap_plano_procedimentos_clinicos") as soap_plano_procedimentos_clinicos,
            json_extract(data, "$.soap_plano_observacoes") as soap_plano_observacoes,
            json_extract(data, "$.soap_avaliacao_observacoes") as soap_avaliacao_observacoes,
            json_extract(data, "$.soap_objetivo_descricao") as soap_objetivo_descricao,
            json_extract(data, "$.notas_observacoes") as notas_observacoes,
            json_extract(data, "$.condicoes") as condicoes,
            json_extract(data, "$.prescricoes") as prescricoes,
            json_extract(data, "$.exames_solicitados") as exames_solicitados,
            json_extract(data, "$.vacinas") as vacinas,
            json_extract(data, "$.alergias_anamnese") as alergias_anamnese,
            json_extract(data, "$.indicadores") as indicadores,
            json_extract(data, "$.encaminhamentos") as encaminhamentos,
            source_updated_at as source_updated_at,
            datalake_loaded_at as datalake_loaded_at
        from source
    )
select * 
from atendimento_continuo