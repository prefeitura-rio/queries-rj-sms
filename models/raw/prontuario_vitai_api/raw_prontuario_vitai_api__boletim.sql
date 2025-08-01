{{
    config(
        materialized='table',
        alias='boletim',
        partition_by={
            "field": "particao_data_boletim",
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
            json_extract_scalar(data, '$.id') AS id,
            json_extract_scalar(data, '$.numero') AS numero,
            json_extract_scalar(data, '$.dataEntrada') AS data_entrada,
            json_extract_scalar(data, '$.dataAlta') AS data_alta,
            json_extract_scalar(data, '$.pacienteId') AS paciente_id,
            json_extract_scalar(data, '$.estabelecimentoId') AS estabelecimento_id,
            json_extract_scalar(data, '$.sobCustodia') AS sob_custodia,
            json_extract_scalar(data, '$.cancelado') AS cancelado,
            json_extract_scalar(data, '$.dataInternacao') AS data_internacao,
            json_extract_scalar(data, '$.isInterno') AS esta_internado,
            json_extract_scalar(data, '$.tipoEntrada') AS tipo_entrada,
            json_extract_scalar(data, '$.tipoAtendimento') AS tipo_atendimento,
            json_extract_scalar(data, '$.estabelecimentoSigla') AS estabelecimento_sigla,
            json_extract_scalar(data, '$.baseurl') AS baseurl,
            json_extract_scalar(data, '$.paciente') AS paciente,
            json_extract_scalar(data, '$.convenio') AS convenio,
            json_extract_scalar(data, '$.detalheAcidente') AS detalhe_acidente,
            json_extract_scalar(data, '$.motivoAtendimento') AS motivo_atendimento,
            json_extract_scalar(data, '$.meioTransporte') AS meio_transporte,
            json_extract_scalar(data, '$.especialidade') AS especialidade,
            json_extract_scalar(data, '$.tipoUnidade') AS tipo_unidade,
            json_extract_scalar(data, '$.paciente.codigo') AS paciente_codigo,
            json_extract_scalar(data, '$.paciente.nome') AS paciente_nome,
            json_extract_scalar(data, '$.paciente.dataHora') AS paciente_data_hora,
            json_extract_scalar(data, '$.paciente.nomeAlternativo') AS paciente_nome_alternativo,
            json_extract_scalar(data, '$.paciente.sexo') AS paciente_sexo,
            json_extract_scalar(data, '$.paciente.dataNascimento') AS paciente_data_nascimento,
            json_extract_scalar(data, '$.paciente.telefone') AS paciente_telefone,
            json_extract_scalar(data, '$.paciente.prontuario') AS paciente_prontuario,
            json_extract_scalar(data, '$.paciente.cpf') AS paciente_cpf,
            json_extract_scalar(data, '$.paciente.cns') AS paciente_cns,
            json_extract_scalar(data, '$.paciente.nacionalidade') AS paciente_nacionalidade,
            json_extract_scalar(data, '$.paciente.dataObito') AS paciente_data_obito,
            json_extract_scalar(data, '$.paciente.celular') AS paciente_celular,
            json_extract_scalar(data, '$.paciente.telefoneExtraUm') AS paciente_telefone_extra_um,
            json_extract_scalar(data, '$.paciente.telefoneExtraDois') AS paciente_telefone_extra_dois,
            json_extract_scalar(data, '$.paciente.email') AS paciente_email,
            json_extract_scalar(data, '$.paciente.cep') AS paciente_cep,
            json_extract_scalar(data, '$.paciente.nomeMae') AS paciente_nome_mae,
            json_extract_scalar(data, '$.paciente.nomePai') AS paciente_nome_pai,
            json_extract_scalar(data, '$.paciente.racaCor') AS paciente_raca_cor,
            json_extract_scalar(data, '$.paciente.tipoLogradouro') AS paciente_tipo_logradouro,
            json_extract_scalar(data, '$.paciente.nomeLogradouro') AS paciente_nome_logradouro,
            json_extract_scalar(data, '$.paciente.numero') AS paciente_numero,
            json_extract_scalar(data, '$.paciente.complemento') AS paciente_complemento,
            json_extract_scalar(data, '$.paciente.bairro') AS paciente_bairro,
            json_extract_scalar(data, '$.paciente.municipio') AS paciente_municipio,
            json_extract_scalar(data, '$.paciente.uf') AS paciente_uf,
            json_extract_scalar(data, '$.paciente.estabelecimentoId') AS paciente_estabelecimento_id,
            json_extract_scalar(data, '$.dataHora') AS updated_at,
            _loaded_at as loaded_at
        FROM source
    ),
    removing_nulls AS (
        select
            {{process_null('id')}} AS id,
            {{process_null('numero')}} AS numero,
            {{process_null('data_entrada')}} AS data_entrada,
            {{process_null('data_alta')}} AS data_alta,
            {{process_null('paciente_id')}} AS paciente_id,
            {{process_null('estabelecimento_id')}} AS estabelecimento_id,
            {{process_null('sob_custodia')}} AS sob_custodia,
            {{process_null('cancelado')}} AS cancelado,
            {{process_null('data_internacao')}} AS data_internacao,
            {{process_null('esta_internado')}} AS esta_internado,
            {{process_null('tipo_entrada')}} AS tipo_entrada,
            {{process_null('tipo_atendimento')}} AS tipo_atendimento,
            {{process_null('estabelecimento_sigla')}} AS estabelecimento_sigla,
            {{process_null('baseurl')}} AS baseurl,
            {{process_null('paciente')}} AS paciente,
            {{process_null('convenio')}} AS convenio,
            {{process_null('detalhe_acidente')}} AS detalhe_acidente,
            {{process_null('motivo_atendimento')}} AS motivo_atendimento,
            {{process_null('meio_transporte')}} AS meio_transporte,
            {{process_null('especialidade')}} AS especialidade,
            {{process_null('tipo_unidade')}} AS tipo_unidade,
            {{process_null('updated_at')}} AS updated_at,
            loaded_at
        from renamed
    ),
    casted as (
        select
            id,
            estabelecimento_id,
            paciente_id,
            numero,
            safe_cast(data_entrada as datetime) as data_entrada,
            safe_cast(data_alta as datetime) as data_alta,
            sob_custodia,
            cancelado,
            esta_internado,
            safe_cast(data_internacao as datetime) as data_internacao,
            tipo_entrada,
            tipo_atendimento,
            estabelecimento_sigla,
            baseurl,
            paciente,
            convenio,
            detalhe_acidente,
            motivo_atendimento,
            meio_transporte,
            especialidade,
            tipo_unidade,
            safe_cast(updated_at as datetime) as updated_at,
            safe_cast(loaded_at as timestamp) as loaded_at,
            date(safe_cast(updated_at as datetime)) as particao_data_boletim
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