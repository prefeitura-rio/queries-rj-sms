{{
    config(
        alias="evolucao",
        materialized="incremental",
        schema='brutos_prontuario_mv',
        unique_key = "id_hci",
        incremental_strategy="insert_overwrite",
        partition_by={
            "field": "data_particao",
            "data_type": "date",
            "granularity": "day",
        },
        tags=['mv']
    )
}}

with

source as (
    select *
    from {{ source("brutos_prontuario_mv_api_staging", "evolucao_continuo") }}
    {% if is_incremental() %}
        where TIMESTAMP_TRUNC(datalake_loaded_at, DAY) >= TIMESTAMP(date_sub(current_date('America/Sao_Paulo'), interval 7 day))
    {% endif %}
),

evolucao_json as (
    select 
        payload_cnes as id_cnes,
        json_extract_scalar(data, '$.numero_atendimento') as numero_atendimento,
        json_extract_scalar(data, '$.diagnostico_cid') as diagnostico_cid,
        json_extract_scalar(data, '$.resumo_internacao') as resumo_internacao,
        json_extract_scalar(data, '$.avaliacao_exames_fisicos') as avaliacao_exames_fisicos,
        json_extract_scalar(data, '$.conduta_adotada') as conduta_adotada,
        json_extract_scalar(data, '$.planejamento_terapeutico') as planejamento_terapeutico,
        json_extract_scalar(data, '$.metas_tratamento') as metas_tratamento,
        json_extract_scalar(data, '$.previsao_alta') as previsao_alta,
        json_extract_scalar(data, '$.profissional_saude_nome') as profissional_saude_nome,
        json_extract_scalar(data, '$.unidade_atendimento_nome') as unidade_atendimento_nome,
        json_extract_scalar(data, '$.nome_paciente') as nome_paciente,
        json_extract_scalar(data, '$.cpf_paciente') as cpf_paciente,
        json_extract_scalar(data, '$.cns_paciente') as cns_paciente,
        json_extract_scalar(data, '$.nome_social_paciente') as nome_social_paciente,
        json_extract_scalar(data, '$.data_nascimento_paciente') as data_nascimento_paciente,
        json_extract_scalar(data, '$.data_atendimento') as data_atendimento,
        json_extract_scalar(data, '$.sexo_paciente') as sexo_paciente,
        json_extract_scalar(data, '$.pcd') as pcd,
        json_extract_scalar(data, '$.nome_mae') as nome_mae,
        json_extract_scalar(data, '$.protocolos') as protocolos,
        json_extract_scalar(data, '$.tipoAtendimento') as tipoAtendimento,
        json_extract_scalar(data, '$.especialidadeAtendimento') as especialidadeAtendimento,
        json_extract_scalar(data, '$.ds_objetivo_enf') as ds_objetivo_enf,
        json_extract_scalar(data, '$.ds_objetivo_nutri') as ds_objetivo_nutri,
        json_extract_scalar(data, '$.ds_objetivo_outros') as ds_objetivo_outros,
        json_extract_scalar(data, '$.dt_prev_alta_setor') as dt_prev_alta_setor,
        json_extract_scalar(data, '$.sn_profilaxia_medicamento') as sn_profilaxia_medicamento,
        json_extract_scalar(data, '$.sn_profilaxia_deambulacao') as sn_profilaxia_deambulacao,
        json_extract_scalar(data, '$.sn_profilaxia_mecanica') as sn_profilaxia_mecanica,
        datalake_loaded_at,
        source_updated_at,
    from source
),

evolucao_renomeado as (
    select
        -- Atendimento
        {{ process_null('numero_atendimento') }} as id_atendimento,
        {{ process_null('id_cnes') }} as id_cnes,
        {{ process_null('unidade_atendimento_nome') }} as estabelecimento_nome,
        safe.parse_datetime('%Y/%m/%d %H:%M:%S', data_atendimento) as atendimento_datahora,
        case 
            when tipoAtendimento like 'A' then 'AMBULATORIAL'
            when tipoAtendimento like 'B' then 'BUSCA ATIVA'
            when tipoAtendimento like 'E' then 'EXTERNO'
            when tipoAtendimento like 'H' then 'HOME CARE'
            when tipoAtendimento like 'I' then 'INTERNAÇÃO'
            when tipoAtendimento like 'S' then 'SUS - AIH'
            when tipoAtendimento like 'U' then 'URGÊNCIA'
            else tipoAtendimento
        end as atendimento_tipo,
        {{ process_null('especialidadeAtendimento') }} as atendimento_especialidade,
        {{ process_null('profissional_saude_nome') }} as profissional_nome,

        -- Paciente
        {{ process_null('nome_paciente') }} as paciente_nome,
        {{ process_null('cpf_paciente') }} as paciente_cpf,
        {{ process_null('cns_paciente') }} as paciente_cns,
        {{ process_null('nome_social_paciente') }} as paciente_nome_social,
        safe.parse_datetime('%Y/%m/%d', data_nascimento_paciente) as paciente_data_nascimento,
        {{ process_null('sexo_paciente') }} as paciente_sexo,
        {{ process_null('pcd') }} as paciente_pcd,
        {{ process_null('nome_mae') }} as paciente_mae_nome,

        -- Evolução
        {{ process_null('diagnostico_cid') }} as diagnostico_cid,
        {{ process_null('resumo_internacao') }} as resumo_internacao,
        {{ process_null('avaliacao_exames_fisicos') }} as avaliacao_exames_fisicos,
        {{ process_null('conduta_adotada') }} as conduta_adotada,
        {{ process_null('planejamento_terapeutico') }} as planejamento_terapeutico,
        {{ process_null('metas_tratamento') }} as metas_tratamento,
        {{ process_null('protocolos') }} as protocolos,

        -- Previsão de Alta
        {{ process_null('previsao_alta') }} as previsao_alta,
        safe.parse_date('%Y-%m-%d', dt_prev_alta_setor) as previsao_alta_setor_data,

        -- Objetivos
        {{ process_null('ds_objetivo_enf') }} as objetivo_enfermagem,
        {{ process_null('ds_objetivo_nutri') }} as objetivo_nutricao,
        {{ process_null('ds_objetivo_outros') }} as objetivo_outros,

        -- Profilaxia
        {{ process_null('sn_profilaxia_medicamento') }} as profilaxia_medicamento,
        {{ process_null('sn_profilaxia_deambulacao') }} as profilaxia_deambulacao,
        {{ process_null('sn_profilaxia_mecanica') }} as profilaxia_mecanica,

        -- Metadados
        datetime(datalake_loaded_at, 'America/Sao_Paulo') as loaded_at,
        parse_datetime('%Y/%m/%d %H:%M:%S', source_updated_at) as updated_at,
        cast(datalake_loaded_at as date) as data_particao
    from evolucao_json
),

evolucao_deduplicado as (
    select *
    from evolucao_renomeado
    qualify row_number() over (partition by id_atendimento, id_cnes order by updated_at desc) = 1
)

select *
from evolucao_deduplicado