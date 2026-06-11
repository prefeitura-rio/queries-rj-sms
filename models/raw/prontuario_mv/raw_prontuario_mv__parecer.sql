{{
    config(
        alias="parecer",
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
        from {{ source("brutos_prontuario_mv_api_staging", "parecer_continuo") }}
        {% if is_incremental() %}
            where
                timestamp_trunc(datalake_loaded_at, day) >= timestamp(
                    date_sub(current_date('America/Sao_Paulo'), interval 7 day)
                )
        {% endif %}
    ),

    parecer_json as (
        select
            payload_cnes as id_cnes,
            json_extract_scalar(data, '$.numero_atendimento') as numero_atendimento,
            json_extract_scalar(
                data, '$.data_solicitacao_parecer'
            ) as data_solicitacao_parecer,
            json_extract_scalar(
                data, '$.prestador_solicitante_nome'
            ) as prestador_solicitante_nome,
            json_extract_scalar(
                data, '$.especialidade_requisitada'
            ) as especialidade_requisitada,
            json_extract_scalar(data, '$.prestador_solicitado') as prestador_solicitado,
            json_extract_scalar(data, '$.situacao_parecer') as situacao_parecer,
            json_extract_scalar(
                data, '$.justificativa_solicitacao'
            ) as justificativa_solicitacao,
            json_extract_scalar(data, '$.descricao_parecer') as descricao_parecer,
            json_extract_scalar(
                data, '$.profissional_saude_nome'
            ) as profissional_saude_nome,
            json_extract_scalar(data, '$.data_parecer') as data_parecer,
            json_extract_scalar(data, '$.servico') as servico,
            json_extract_scalar(data, '$.cid') as cid,
            json_extract_scalar(
                data, '$.unidade_atendimento_nome'
            ) as unidade_atendimento_nome,
            json_extract_scalar(data, '$.nome_paciente') as nome_paciente,
            json_extract_scalar(data, '$.cpf_paciente') as cpf_paciente,
            json_extract_scalar(data, '$.cns_paciente') as cns_paciente,
            json_extract_scalar(data, '$.nome_social_paciente') as nome_social_paciente,
            json_extract_scalar(data, '$.data_atendimento') as data_atendimento,
            json_extract_scalar(
                data, '$.data_nascimento_paciente'
            ) as data_nascimento_paciente,
            json_extract_scalar(data, '$.sexo_paciente') as sexo_paciente,
            json_extract_scalar(data, '$.pcd') as pcd,
            json_extract_scalar(data, '$.nome_mae') as nome_mae,
            json_extract_scalar(data, '$.tipoAtendimento') as tipoatendimento,
            json_extract_scalar(
                data, '$.especialidadeAtendimento'
            ) as especialidadeatendimento,
            datalake_loaded_at,
            source_updated_at
        from source
    ),

    -- Aplique as macros de processamento de nulo na CTE abaixo
    parecer_renomeado as (
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
                then 'AMBULATORIAL'
                when tipoatendimento like 'E'
                then 'EXTERNO'
                when tipoatendimento like 'I'
                then 'INTERNAÇÃO'
                when tipoatendimento like 'U'
                then 'URGÊNCIA'
                else tipoatendimento
            end as atendimento_tipo,
            {{ process_null("especialidadeAtendimento") }} as atendimento_especialidade,
            {{ process_null("profissional_saude_nome") }} as profissional_nome,

            -- Paciente
            {{ process_null("nome_paciente") }} as paciente_nome,
            {{ process_null("cpf_paciente") }} as paciente_cpf,
            {{ process_null("cns_paciente") }} as paciente_cns,
            {{ process_null("nome_social_paciente") }} as paciente_nome_social,
            safe.parse_date(
                '%Y/%m/%d', data_nascimento_paciente
            ) as paciente_data_nascimento,
            {{ process_null("sexo_paciente") }} as paciente_sexo,
            {{ process_null("pcd") }} as paciente_pcd,
            {{ process_null("nome_mae") }} as paciente_mae_nome,

            -- Parecer
            safe.parse_datetime(
                '%Y/%m/%d %H:%M:%S', data_solicitacao_parecer
            ) as solicitacao_parecer_datahora,
            {{ process_null("prestador_solicitante_nome") }} as solicitante_nome,
            {{ process_null("especialidade_requisitada") }}
            as especialidade_requisitada,
            {{ process_null("prestador_solicitado") }} as solicitado_nome,
            {{ process_null("situacao_parecer") }} as parecer_situacao,
            {{ process_null("justificativa_solicitacao") }}
            as solicitacao_justificativa,
            {{ process_null("descricao_parecer") }} as parecer_descricao,
            safe.parse_datetime('%Y/%m/%d %H:%M:%S', data_parecer) as parecer_datahora,
            {{ process_null("servico") }} as servico,
            {{ process_null("cid") }} as cid,

            -- Metadados
            datetime(datalake_loaded_at, 'America/Sao_Paulo') as loaded_at,
            parse_datetime('%Y/%m/%d %H:%M:%S', source_updated_at) as updated_at,
            cast(datalake_loaded_at as date) as data_particao
        from parecer_json
    ),

    parecer_deduplicado as (
        select *
        from parecer_renomeado
        qualify
            row_number() over (
                partition by id_atendimento, id_cnes order by updated_at desc
            )
            = 1
    )

select *
from parecer_deduplicado
