{{
    config(
        alias="receituario_simples",
        materialized="table",
        schema='brutos_prontuario_sarah',
    )
}}

with source as (
    select 
        * 
    from {{ source("brutos_prontuario_sarah_api_staging", "atendimento_continuo") }}
    qualify row_number() over (partition by source_id order by datalake_loaded_at desc) = 1
),

    receituario_filtrado as (
        select 
            source_id,
            payload_cnes,
            doc as doc_receituario
        from source, 
        unnest(json_extract_array(data, '$.documentos')) AS doc
        where json_value(doc, '$.tipo') = 'RECEITUÁRIO SIMPLES'
    ),

    receituario_itens as (
        select
            source_id,
            payload_cnes,
            doc_receituario,
            item as item_medicamento
        from receituario_filtrado,
        unnest(json_extract_array(doc_receituario, '$.dados.itens')) as item
    ),

    receituario as (
        select
            source_id,
            payload_cnes as id_cnes,

            json_extract_scalar(doc_receituario, '$.id') as id_receituario,
            json_extract_scalar(doc_receituario, '$.numero') as receituario_numero,
            json_extract_scalar(doc_receituario, '$.datahora') as receituario_datahora,

            json_extract_scalar(doc_receituario, '$.dados.data') as receituario_data,
            {{base64_to_string("json_extract_scalar(doc_receituario, '$.dados.orientacoes_gerais')")}} as orientacoes_gerais,

            json_extract_scalar(item_medicamento, '$.uso') as medicamento_uso,
            json_extract_scalar(item_medicamento, '$.inscricao') as medicamento_inscricao,
            json_extract_scalar(item_medicamento, '$.subscricao') as medicamento_subscricao,
            json_extract_scalar(item_medicamento, '$.adscricao') as medicamento_adscricao,
            json_extract_scalar(item_medicamento, '$.orientacao') as medicamento_orientacao,

            json_extract_scalar(doc_receituario, '$.profissional.nome') as profissional_nome,
            json_extract_scalar(doc_receituario, '$.profissional.cpf') as profissional_cpf,
            json_extract_scalar(doc_receituario, '$.profissional.cns') as profissional_cns,
            json_extract_scalar(doc_receituario, '$.profissional.cbo') as profissional_cbo

        from receituario_itens
    ),

    cast_rename as (
        select
            safe_cast({{ process_null('source_id') }} as string) as source_id,
            safe_cast({{ process_null('id_cnes') }} as string) as id_cnes,

            safe_cast({{ process_null('id_receituario') }} as string) as id_receituario,
            safe_cast({{ process_null('receituario_numero') }} as string) as receituario_numero,
            safe_cast({{ process_null('receituario_datahora') }} as datetime) as receituario_datahora,
            safe_cast({{ process_null('receituario_data') }} as date) as receituario_data,
            
            safe_cast({{ process_null('orientacoes_gerais') }} as string) as orientacoes_gerais,
            safe_cast({{ process_null('medicamento_uso') }} as string) as medicamento_uso,
            safe_cast({{ process_null('medicamento_inscricao') }} as string) as medicamento_inscricao,
            safe_cast({{ process_null('medicamento_subscricao') }} as string) as medicamento_subscricao,
            safe_cast({{ process_null('medicamento_adscricao') }} as string) as medicamento_adscricao,
            safe_cast({{ process_null('medicamento_orientacao') }} as string) as medicamento_orientacao,

            safe_cast({{ process_null('profissional_nome') }} as string) as profissional_nome,
            safe_cast({{ process_null('profissional_cpf') }} as string) as profissional_cpf,
            safe_cast({{ process_null('profissional_cns') }} as string) as profissional_cns,
            safe_cast({{ process_null('profissional_cbo') }} as string) as profissional_cbo
        from receituario
    )

select * from cast_rename