{{
    config(
        materialized='table',
        alias='atendimento',
        partition_by={
            "field": "particao_data_atendimento",
            "data_type": "date",
            "granularity": "day",
        },
    )
}}

WITH

    source AS (
        SELECT * FROM {{ source('brutos_prontuario_vitai_api_staging', 'atendimento') }}
    ),
    renamed AS (
        SELECT
            json_extract_scalar(data, '$.id') AS id,
            json_extract_scalar(data, '$.boletimId') AS boletim_id,
            json_extract_scalar(data, '$.pacienteId') AS paciente_id,
            json_extract_scalar(data, '$.profissionalId') AS profissional_id,
            json_extract_scalar(data, '$.estabelecimentoId') AS estabelecimento_id,
            json_extract_scalar(data, '$.codigo') AS codigo,
            json_extract_scalar(data, '$.tipoAtendimento') AS tipo_atendimento,
            json_extract_scalar(data, '$.dthrInicio') AS dthr_inicio,
            json_extract_scalar(data, '$.dthrFim') AS dthr_fim,
            json_extract_scalar(data, '$.anamnese') AS anamnese,
            json_extract_scalar(data, '$.cidCodigo') AS cid_codigo,
            json_extract_scalar(data, '$.cid') AS cid,
            json_extract_scalar(data, '$.especialidade') AS especialidade,
            json_extract_scalar(data, '$.dataHora') AS updated_at,
            _loaded_at as loaded_at
        FROM source
    ),
    removing_nulls AS (
        select
            {{process_null('id')}} AS id,
            {{process_null('boletim_id')}} AS boletim_id,
            {{process_null('paciente_id')}} AS paciente_id,
            {{process_null('profissional_id')}} AS profissional_id,
            {{process_null('estabelecimento_id')}} AS estabelecimento_id,
            {{process_null('codigo')}} AS codigo,
            {{process_null('tipo_atendimento')}} AS tipo_atendimento,
            {{process_null('dthr_inicio')}} AS dthr_inicio,
            {{process_null('dthr_fim')}} AS dthr_fim,
            {{process_null('anamnese')}} AS anamnese,
            {{process_null('cid_codigo')}} AS cid_codigo,
            {{process_null('cid')}} AS cid,
            {{process_null('especialidade')}} AS especialidade,
            {{process_null('updated_at')}} AS updated_at,
            loaded_at
        from renamed
    ),
    casted as (
        select
            id,
            boletim_id,
            paciente_id,
            profissional_id,
            estabelecimento_id,
            codigo,
            tipo_atendimento,
            safe_cast(dthr_inicio as datetime) as dthr_inicio,
            safe_cast(dthr_fim as datetime) as dthr_fim,
            anamnese,
            cid_codigo,
            cid,
            especialidade,
            
            safe_cast(updated_at as datetime) as updated_at,
            safe_cast(loaded_at as timestamp) as loaded_at,
            date(safe_cast(updated_at as datetime)) as particao_data_atendimento
        from removing_nulls
    ),
    deduplicated AS (
        select
            *
        from casted
        qualify row_number() over (partition by id order by updated_at desc) = 1
    )
SELECT * 
FROM deduplicated