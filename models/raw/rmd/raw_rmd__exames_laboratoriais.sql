{{
    config(
        alias="exames_laboratoriais",
        materialized="incremental",
        incremental_strategy="insert_overwrite",
        unique_key=['id', 'fornecedor_id'],
        schema="brutos_rmd",
        partition_by={
            "field": "data_particao",
            "data_type": "date",
            "granularity": "month"
        },
        cluster_by='fornecedor_nome'
    )
}}

{% set last_partition = get_last_partition_date(this) %}

with
    source as (
        select *
        from {{ source('brutos_rmd_staging', 'exames_laboratoriais') }}
        {% if is_incremental() and last_partition %}
        where date(data_particao) >= date('{{ last_partition }}')
        {% endif %}
    ),

    parsed as (
        select

            {{ process_null('id') }}                                                              as id,
            {{ process_null('tipo_recurso_id') }}                                                 as tipo_recurso_id,
            {{ process_null('fornecedor_id') }}                                                   as fornecedor_id,
            {{ process_null('fornecedor_nome') }}                                                 as fornecedor_nome,
            cast({{ process_null('validado') }} as boolean)                                       as validado,
            {{ process_null('erros_validacao') }}                                                 as erros_validacao,
            safe_cast({{ process_null('recebido_em') }} as datetime)                             as recebido_em,
            safe_cast({{ process_null('_extracted_at') }} as datetime)                           as loaded_at,
            safe_cast({{ process_null('data_particao') }} as date)                               as data_particao,

            -- exam identification
            {{ process_null("json_extract_scalar(dados, '$.id_exame')") }}                       as id_exame,
            {{ process_null("json_extract_scalar(dados, '$.exame_solicitacao_id')") }}           as exame_solicitacao_id,
            {{ process_null("json_extract_scalar(dados, '$.exame_nome')") }}                     as exame_nome,
            {{ process_null("json_extract_scalar(dados, '$.exame_tipo')") }}                     as exame_tipo,
            {{ process_null("json_extract_scalar(dados, '$.exame_metodo')") }}                   as exame_metodo,
            {{ process_null("json_extract_scalar(dados, '$.exame_status')") }}                   as exame_status,

            -- exam dates
            safe_cast(
                {{ process_null("json_extract_scalar(dados, '$.exame_data')") }}
                as datetime
            )                                                                                     as exame_data,
            safe_cast(
                {{ process_null("json_extract_scalar(dados, '$.data_da_solicitacao_do_exame_e_hora')") }}
                as datetime
            )                                                                                     as exame_solicitacao_data_hora,
            safe_cast(
                {{ process_null("json_extract_scalar(dados, '$.data_hora_da_coleta')") }}
                as datetime
            )                                                                                     as exame_coleta_data_hora,
            safe_cast(
                {{ process_null("json_extract_scalar(dados, '$.data_hora_do_laudo_do_exame')") }}
                as datetime
            )                                                                                     as exame_laudo_data_hora,

            -- exam result
            {{ process_null("json_extract_scalar(dados, '$.exame_resultado_classificacao')") }}  as exame_resultado_classificacao,
            {{ process_null("json_extract_scalar(dados, '$.exame_laudo_descricao')") }}          as exame_laudo_descricao,
            {{ process_null("json_extract_scalar(dados, '$.exame_resultado_laudo')") }}          as exame_resultado_laudo,

            -- exame_resultado_valor: array of structs {nome, valor, unidade, referencia}
            array(
                select as struct
                    json_extract_scalar(item, '$.nome')                        as nome,
                    safe_cast(json_extract_scalar(item, '$.valor') as float64) as valor,
                    json_extract_scalar(item, '$.unidade')                     as unidade,
                    json_extract_scalar(item, '$.referencia')                  as referencia
                from unnest(
                    ifnull(json_extract_array(dados, '$.exame_resultado_valor'), [])
                ) as item
            )                                                                                     as exame_resultado_valor,

            -- patient
            {{ process_null("json_extract_scalar(dados, '$.paciente_cpf')") }}                   as paciente_cpf,
            {{ process_null("json_extract_scalar(dados, '$.paciente_cns')") }}                   as paciente_cns,
            {{ process_null("json_extract_scalar(dados, '$.paciente_sexo')") }}                  as paciente_sexo,
            safe_cast(
                {{ process_null("json_extract_scalar(dados, '$.paciente_nascimento_data')") }}
                as date
            )                                                                                     as paciente_nascimento_data,
            {{ process_null("json_extract_scalar(dados, '$.nome_do_paciente')") }}               as paciente_nome,

            -- establishment
            {{ process_null("json_extract_scalar(dados, '$.estabelecimento_nome')") }}           as estabelecimento_nome,

            -- requesting professional
            {{ process_null("json_extract_scalar(dados, '$.profissional_solicitante_nome')") }}  as profissional_solicitante_nome,
            {{ process_null("json_extract_scalar(dados, '$.profissional_solicitante_cpf')") }}   as profissional_solicitante_cpf,
            {{ process_null("json_extract_scalar(dados, '$.profissional_solicitante_crm')") }}   as profissional_solicitante_crm,
            {{ process_null("json_extract_scalar(dados, '$.profissional_solicitante_cbo')") }}   as profissional_solicitante_cbo,

            -- signing professional (laudista)
            {{ process_null("json_extract_scalar(dados, '$.profissional_laudista_nome')") }}     as profissional_laudista_nome,
            {{ process_null("json_extract_scalar(dados, '$.profissional_laudista_cpf')") }}      as profissional_laudista_cpf,
            {{ process_null("json_extract_scalar(dados, '$.profissional_laudista_cbo')") }}      as profissional_laudista_cbo

        from source
    ),

    deduplicado as (
        select *
        from parsed
        qualify row_number() over (
            partition by id, fornecedor_id
            order by loaded_at desc
        ) = 1
    )

select *
from deduplicado
