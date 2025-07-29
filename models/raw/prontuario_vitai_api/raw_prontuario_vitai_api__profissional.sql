{{
    config(
        materialized='table',
        alias='profissional',
        partition_by={
            "field": "particao_data_atualizacao",
            "data_type": "date",
            "granularity": "day",
        },
    )
}}

WITH

    source AS (
        SELECT * FROM {{ source('brutos_prontuario_vitai_api_staging', 'profissional') }}
    ),
    renamed AS (
        SELECT
            json_extract_scalar(data, '$.id') AS id,
            json_extract_scalar(data, '$.estabelecimentoId') AS estabelecimento_id,
            json_extract_scalar(data, '$.cbo') AS cbo,
            json_extract_scalar(data, '$.cboDescricao') AS cbo_descricao,
            json_extract_scalar(data, '$.codigo') AS codigo,
            json_extract_scalar(data, '$.nome') AS nome,
            json_extract_scalar(data, '$.cns') AS cns,
            json_extract_scalar(data, '$.numeroConselho') AS numero_conselho,
            json_extract_scalar(data, '$.ufConselho') AS uf_conselho,
            json_extract_scalar(data, '$.cpf') AS cpf,
            json_extract_scalar(data, '$.dataHora') AS updated_at,
            _loaded_at as loaded_at
        FROM source
    ),
    removing_nulls AS (
        select
            {{process_null('id')}} AS id,
            {{process_null('estabelecimento_id')}} AS estabelecimento_id,
            {{process_null('cbo')}} AS cbo,
            {{process_null('cbo_descricao')}} AS cbo_descricao,
            {{process_null('codigo')}} AS codigo,
            {{process_null('nome')}} AS nome,
            {{process_null('cns')}} AS cns,
            {{process_null('numero_conselho')}} AS numero_conselho,
            {{process_null('uf_conselho')}} AS uf_conselho,
            {{process_null('cpf')}} AS cpf,
            {{process_null('updated_at')}} AS updated_at,
            loaded_at
        from renamed
    ),
    casted as (
        select
            id,
            estabelecimento_id,
            cbo,
            cbo_descricao,
            codigo,
            nome,
            cns,
            numero_conselho,
            uf_conselho,
            cpf,

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