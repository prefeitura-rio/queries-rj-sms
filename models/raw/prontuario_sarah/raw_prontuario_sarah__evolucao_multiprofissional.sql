{{
    config(
        alias="evolucao_multiprofissional",
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

    evolucao_filtrado as (
        select 
            source_id,
            payload_cnes,
            doc as doc_evolucao
        from source, 
        unnest(json_extract_array(data, '$.documentos')) AS doc
        where json_value(doc, '$.tipo') = 'EVOLUÇÃO MULTIPROFISSIONAL'
    ),

    evolucao as (
        select
            source_id,
            payload_cnes as id_cnes,

            json_extract_scalar(doc_evolucao, '$.id') as id_evolucao,
            json_extract_scalar(doc_evolucao, '$.numero') as evolucao_numero,
            json_extract_scalar(doc_evolucao, '$.datahora') as evolucao_datahora,

            -- Conteúdo clínico (Decodificado)
            {{ base64_to_string("json_extract_scalar(doc_evolucao, '$.dados.texto')") }} as texto_evolucao,
            json_extract_scalar(doc_evolucao, '$.dados.conduta') as conduta_evolucao,

            -- Profissional
            json_extract_scalar(doc_evolucao, '$.profissional.nome') as profissional_nome,
            json_extract_scalar(doc_evolucao, '$.profissional.cpf') as profissional_cpf,
            json_extract_scalar(doc_evolucao, '$.profissional.cns') as profissional_cns,
            json_extract_scalar(doc_evolucao, '$.profissional.cbo') as profissional_cbo

        from evolucao_filtrado
    ),

    cast_rename as (
        -- 4. Tipagem e limpeza final
        select
            safe_cast({{ process_null('source_id') }} as string) as source_id,
            safe_cast({{ process_null('id_cnes') }} as string) as id_cnes,

            safe_cast({{ process_null('id_evolucao') }} as string) as id_documento,
            safe_cast({{ process_null('evolucao_numero') }} as string) as evolucao_numero,
            safe_cast({{ process_null('evolucao_datahora') }} as datetime) as evolucao_datahora,
            
            safe_cast({{ process_null('texto_evolucao') }} as string) as texto_evolucao,
            safe_cast({{ process_null('conduta_evolucao') }} as string) as conduta_evolucao,

            safe_cast({{ process_null('profissional_nome') }} as string) as profissional_nome,
            safe_cast({{ process_null('profissional_cpf') }} as string) as profissional_cpf,
            safe_cast({{ process_null('profissional_cns') }} as string) as profissional_cns,
            safe_cast({{ process_null('profissional_cbo') }} as string) as profissional_cbo
        from evolucao
    )

select * from cast_rename