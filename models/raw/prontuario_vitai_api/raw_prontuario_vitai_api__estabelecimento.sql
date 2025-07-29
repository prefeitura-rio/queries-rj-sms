{{
    config(
        materialized='table',
        alias='estabelecimento',
        partition_by={
            "field": "particao_data_atualizacao",
            "data_type": "date",
            "granularity": "day",
        },
    )
}}

WITH

    source AS (
        SELECT * FROM {{ source('brutos_prontuario_vitai_api_staging', 'estabelecimento') }}
    ),
    renamed AS (
        SELECT
            json_extract_scalar(data, '$.id') AS id,
            json_extract_scalar(data, '$.codigo') AS codigo,
            json_extract_scalar(data, '$.nome') AS nome,
            json_extract_scalar(data, '$.cnes') AS cnes,
            json_extract_scalar(data, '$.sigla') AS sigla,
            json_extract_scalar(data, '$.cnpj') AS cnpj,
            json_extract_scalar(data, '$.dataHora') AS updated_at,
            _loaded_at as loaded_at
        FROM source
    ),
    removing_nulls AS (
        select
            {{process_null('id')}} AS id,
            {{process_null('codigo')}} AS codigo,
            {{process_null('nome')}} AS nome,
            {{process_null('cnes')}} AS cnes,
            {{process_null('sigla')}} AS sigla,
            {{process_null('cnpj')}} AS cnpj,
            {{process_null('updated_at')}} AS updated_at,
            loaded_at
        from renamed
    ),
    casted as (
        select
            id,
            codigo,
            nome,
            cnes,
            sigla,
            cnpj,

            safe_cast(updated_at as datetime) as updated_at,
            safe_cast(loaded_at as timestamp) as loaded_at,
            date(safe_cast(updated_at as datetime)) as particao_data_atualizacao
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