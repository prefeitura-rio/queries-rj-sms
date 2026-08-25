{{
    config(
        alias="alta",
        materialized="incremental",
        schema="brutos_prontuario_mv",
        unique_key="id_hci",
        incremental_strategy="merge",
        partition_by={
            "field": "data_particao",
            "data_type": "date",
            "granularity": "day",
        },
        tags=["mv"],
    )
}}


with

    source as (
        select *
        from {{ source("brutos_prontuario_mv_api_staging", "alta_continuo") }}
        {% if is_incremental() %}
            where
                timestamp_trunc(datalake_loaded_at, day) >= timestamp(
                    date_sub(current_date('America/Sao_Paulo'), interval 7 day)
                )
        {% endif %}
    ),

    alta_json as (
        select
            payload_cnes as id_cnes,
            json_extract_scalar(data, '$.numero_atendimento') as numero_atendimento,
            json_extract_scalar(data, '$.cid_principal') as cid_principal,
            json_extract_scalar(
                data, '$.procedimentos_realizados'
            ) as procedimentos_realizados,
            json_extract_scalar(data, '$.resultados_obtidos') as resultados_obtidos,
            json_extract_scalar(
                data, '$.medicamentos_utilizados'
            ) as medicamentos_utilizados,
            json_extract_scalar(data, '$.evolucao_paciente') as evolucao_paciente,
            json_extract_scalar(
                data, '$.plano_alta_orientacao_nutricionista'
            ) as plano_alta_orientacao_nutricionista,
            json_extract_scalar(
                data, '$.plano_alta_orientacao_enfermagem'
            ) as plano_alta_orientacao_enfermagem,
            json_extract_scalar(
                data, '$.plano_alta_orientacao_fisioterapia'
            ) as plano_alta_orientacao_fisioterapia,
            json_extract_scalar(
                data, '$.plano_alta_orientacao_psicologia'
            ) as plano_alta_orientacao_psicologia,
            json_extract_scalar(
                data, '$.orientacao_medica_alta'
            ) as orientacao_medica_alta,
            json_extract_scalar(data, '$.local_transferencia') as local_transferencia,
            json_extract_scalar(
                data, '$.data_hora_alta_medica'
            ) as data_hora_alta_medica,
            json_extract_scalar(data, '$.dias_internado') as dias_internado,
            json_extract_scalar(data, '$.necessidade_retorno') as necessidade_retorno,
            json_extract_scalar(
                data, '$.profissional_saude_nome'
            ) as profissional_saude_nome,
            json_extract_scalar(
                data, '$.unidade_atendimento_nome'
            ) as unidade_atendimento_nome,
            json_extract_scalar(data, '$.cid_principal_ativo') as cid_principal_ativo,
            json_extract_scalar(
                data, '$.cid_principal_inativo'
            ) as cid_principal_inativo,
            json_extract_scalar(data, '$.retorno_agendado_nao') as retorno_agendado_nao,
            json_extract_scalar(data, '$.retorno_agendado_sim') as retorno_agendado_sim,
            json_extract_scalar(data, '$.nome_paciente') as nome_paciente,
            json_extract_scalar(data, '$.cpf_paciente') as cpf_paciente,
            json_extract_scalar(data, '$.cns_paciente') as cns_paciente,
            json_extract_scalar(data, '$.nome_social_paciente') as nome_social_paciente,
            json_extract_scalar(
                data, '$.data_nascimento_paciente'
            ) as data_nascimento_paciente,
            json_extract_scalar(data, '$.data_atendimento') as data_atendimento,
            json_extract_scalar(data, '$.sexo_paciente') as sexo_paciente,
            json_extract_scalar(data, '$.pcd') as pcd,
            json_extract_scalar(data, '$.tipoAtendimento') as tipoatendimento,
            json_extract_scalar(
                data, '$.especialidadeAtendimento'
            ) as especialidadeatendimento,
            json_extract_scalar(data, '$.dh_fechamento') as dh_fechamento,
            source_updated_at,
            datalake_loaded_at
        from source
    ),

    alta_renomeado as (
        select
            -- Atendimento
            {{ process_null("numero_atendimento") }} as id_atendimento,
            {{ process_null("id_cnes") }} as id_cnes,
            {{ process_null("unidade_atendimento_nome") }} as estabelecimento_nome,
            safe.parse_datetime(
                '%Y/%m/%d %H:%M:%S', data_atendimento
            ) as atendimento_datahora,
            case
                when tipoatendimento like 'A'
                then 'Ambulatorial'
                when tipoatendimento like 'E'
                then 'Externo'
                when tipoatendimento like 'I'
                then 'Internação'
                when tipoatendimento like 'U'
                then 'Urgência'
                else tipoatendimento
            end as atendimento_tipo,
            {{ process_null("especialidadeAtendimento") }} as atendimento_especialidade,
            {{ process_null("profissional_saude_nome") }} as profissional_nome,
            {{ process_null("cid_principal") }} as cid_principal,
            {{ process_null("cid_principal_ativo") }} as cid_principal_ativo,
            {{ process_null("cid_principal_inativo") }} as cid_principal_inativo,
            {{ process_null("procedimentos_realizados") }} as procedimentos_realizados,
            {{ process_null("resultados_obtidos") }} as resultados_obtidos,
            {{ process_null("medicamentos_utilizados") }} as medicamentos_utilizados,
            {{ process_null("evolucao_paciente") }} as evolucao_paciente,

            -- Paciente
            {{ process_null("nome_paciente") }} as paciente_nome,
            {{ process_null("cpf_paciente") }} as paciente_cpf,
            {{ process_null("cns_paciente") }} as paciente_cns,
            {{ process_null("nome_social_paciente") }} as paciente_nome_social,
            safe.parse_datetime(
                '%Y/%m/%d %H:%M:%S', data_nascimento_paciente
            ) as paciente_data_nascimento,
            {{ process_null("sexo_paciente") }} as paciente_sexo,
            {{ process_null("pcd") }} as paciente_pcd,

            -- Alta
            safe.parse_datetime(
                '%Y/%m/%d %H:%M:%S', data_hora_alta_medica
            ) as alta_medica_datahora,
            safe.parse_datetime(
                '%Y/%m/%d %H:%M:%S', dh_fechamento
            ) as alta_datahora_fechamento,
            safe_cast({{ process_null("dias_internado") }} as int64) as dias_internado,
            {{ process_null("local_transferencia") }} as local_transferencia,

            -- Plano de Alta
            {{ process_null("plano_alta_orientacao_nutricionista") }}
            as plano_alta_orientacao_nutricionista,
            {{ process_null("plano_alta_orientacao_enfermagem") }}
            as plano_alta_orientacao_enfermagem,
            {{ process_null("plano_alta_orientacao_fisioterapia") }}
            as plano_alta_orientacao_fisioterapia,
            {{ process_null("plano_alta_orientacao_psicologia") }}
            as plano_alta_orientacao_psicologia,
            {{ process_null("orientacao_medica_alta") }} as orientacao_medica,
            {{ process_null("necessidade_retorno") }} as necessidade_retorno,
            {{ process_null("retorno_agendado_nao") }} as retorno_agendado_nao,
            {{ process_null("retorno_agendado_sim") }} as retorno_agendado_sim,

            -- Metadados
            datetime(datalake_loaded_at, 'America/Sao_Paulo') as loaded_at,
            parse_datetime('%Y/%m/%d %H:%M:%S', source_updated_at) as updated_at,
            cast(datalake_loaded_at as date) as data_particao
        from alta_json
    ),

    alta_deduplicado as (
        select *
        from alta_renomeado
        qualify
            row_number() over (
                partition by id_atendimento, id_cnes order by updated_at desc
            )
            = 1
    )

select
    {{ dbt_utils.generate_surrogate_key(["id_atendimento", "id_cnes"]) }} as id_hci, *
from alta_deduplicado
