{{
    config(
        materialized='table',
        alias='paciente',
        partition_by={
            "field": "particao_data_atualizacao",
            "data_type": "date",
            "granularity": "day",
        },
    )
}}

WITH

    source AS (
        SELECT * FROM {{ source('brutos_prontuario_vitai_api_staging', 'boletim') }}
    ),
    renamed AS (
        SELECT
            json_extract_scalar(data, '$.paciente.id') AS id,
            json_extract_scalar(data, '$.paciente.estabelecimentoId') AS estabelecimento_id,
            json_extract_scalar(data, '$.paciente.codigo') AS codigo,
            json_extract_scalar(data, '$.paciente.nome') AS nome,
            json_extract_scalar(data, '$.paciente.nomeAlternativo') AS nome_alternativo,
            json_extract_scalar(data, '$.paciente.sexo') AS sexo,
            json_extract_scalar(data, '$.paciente.dataNascimento') AS data_nascimento,
            json_extract_scalar(data, '$.paciente.telefone') AS telefone,
            json_extract_scalar(data, '$.paciente.prontuario') AS prontuario,
            json_extract_scalar(data, '$.paciente.cpf') AS cpf,
            json_extract_scalar(data, '$.paciente.cns') AS cns,
            json_extract_scalar(data, '$.paciente.nacionalidade') AS nacionalidade,
            json_extract_scalar(data, '$.paciente.dataObito') AS data_obito,
            json_extract_scalar(data, '$.paciente.celular') AS celular,
            json_extract_scalar(data, '$.paciente.telefoneExtraUm') AS telefone_extra_um,
            json_extract_scalar(data, '$.paciente.telefoneExtraDois') AS telefone_extra_dois,
            json_extract_scalar(data, '$.paciente.email') AS email,
            json_extract_scalar(data, '$.paciente.cep') AS cep,
            json_extract_scalar(data, '$.paciente.nomeMae') AS nome_mae,
            json_extract_scalar(data, '$.paciente.nomePai') AS nome_pai,
            json_extract_scalar(data, '$.paciente.racaCor') AS raca_cor,
            json_extract_scalar(data, '$.paciente.tipoLogradouro') AS tipo_logradouro,
            json_extract_scalar(data, '$.paciente.nomeLogradouro') AS nome_logradouro,
            json_extract_scalar(data, '$.paciente.numero') AS numero,
            json_extract_scalar(data, '$.paciente.complemento') AS complemento,
            json_extract_scalar(data, '$.paciente.bairro') AS bairro,
            json_extract_scalar(data, '$.paciente.municipio') AS municipio,
            json_extract_scalar(data, '$.paciente.uf') AS uf,
            json_extract_scalar(data, '$.paciente.dataHora') AS updated_at,
            _loaded_at as loaded_at
        FROM source
    ),
    removing_nulls AS (
        select
            {{process_null('id')}} AS id,
            {{process_null('estabelecimento_id')}} AS estabelecimento_id,
            {{process_null('codigo')}} AS codigo,
            {{process_null('nome')}} AS nome,
            {{process_null('nome_alternativo')}} AS nome_alternativo,
            {{process_null('sexo')}} AS sexo,
            {{process_null('data_nascimento')}} AS data_nascimento,
            {{process_null('telefone')}} AS telefone,
            {{process_null('prontuario')}} AS prontuario,
            {{process_null('cpf')}} AS cpf,
            {{process_null('cns')}} AS cns,
            {{process_null('nacionalidade')}} AS nacionalidade,
            {{process_null('data_obito')}} AS data_obito,
            {{process_null('celular')}} AS celular,
            {{process_null('telefone_extra_um')}} AS telefone_extra_um,
            {{process_null('telefone_extra_dois')}} AS telefone_extra_dois,
            {{process_null('email')}} AS email,
            {{process_null('cep')}} AS cep,
            {{process_null('nome_mae')}} AS nome_mae,
            {{process_null('nome_pai')}} AS nome_pai,
            {{process_null('raca_cor')}} AS raca_cor,
            {{process_null('tipo_logradouro')}} AS tipo_logradouro,
            {{process_null('nome_logradouro')}} AS nome_logradouro,
            {{process_null('numero')}} AS numero,
            {{process_null('complemento')}} AS complemento,
            {{process_null('bairro')}} AS bairro,
            {{process_null('municipio')}} AS municipio,
            {{process_null('uf')}} AS uf,
            {{process_null('updated_at')}} AS updated_at,
            loaded_at
        from renamed
    ),
    casted as (
        select
            id,
            estabelecimento_id,
            codigo,
            nome,
            nome_alternativo,
            sexo,
            safe_cast(data_nascimento as datetime) as data_nascimento,
            telefone,
            prontuario,
            cpf,
            cns,
            nacionalidade,
            safe_cast(data_obito as datetime) as data_obito,
            celular,
            telefone_extra_um,
            telefone_extra_dois,
            email,
            cep,
            nome_mae,
            nome_pai,
            raca_cor,
            tipo_logradouro,
            nome_logradouro,
            numero,
            complemento,
            bairro,
            municipio,
            uf,
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