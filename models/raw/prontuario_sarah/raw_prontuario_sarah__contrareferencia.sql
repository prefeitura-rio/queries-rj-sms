{{
    config(
        alias="contrareferencia",
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

    contrareferencia_filtrado as (
        select 
            source_id,
            doc as doc_contrareferencia
        from source, 
        unnest(json_extract_array(data, '$.documentos')) AS doc
        where json_value(doc, '$.tipo') = 'CONTRAREFERÊNCIA'
    ),

    contrareferencia as (
        select
            source_id,
            json_extract_scalar(doc_contrareferencia, '$.dados.unidade.cnes') as id_cnes,
            json_extract_scalar(doc_contrareferencia, '$.dados.unidade.nome') as unidade_nome,
            json_extract_scalar(doc_contrareferencia, '$.dados.unidade.municipio') as unidade_municipio,

            json_extract_scalar(doc_contrareferencia, '$.id') as id_contrareferencia,
            json_extract_scalar(doc_contrareferencia, '$.numero') as contraferencia_numero,
            json_extract_scalar(doc_contrareferencia, '$.datahora') as contrareferencia_datahora,
            

            {{ base64_to_string("json_extract_scalar(doc_contrareferencia, '$.dados.motivo')") }} as motivo,
            {{ base64_to_string("json_extract_scalar(doc_contrareferencia, '$.dados.resultados')") }} as resultados,
            {{ base64_to_string("json_extract_scalar(doc_contrareferencia, '$.dados.conduta')") }} as conduta,
            {{ base64_to_string("json_extract_scalar(doc_contrareferencia, '$.dados.impressao')") }} as impressao,
            {{ base64_to_string("json_extract_scalar(doc_contrareferencia, '$.dados.conduta_seguimento')") }} as conduta_seguimento,
            {{ base64_to_string("json_extract_scalar(doc_contrareferencia, '$.dados.resumo')") }} as resumo,

            json_extract_scalar(doc_contrareferencia, '$.dados.cid_principal') as cid_principal,
            json_extract_scalar(doc_contrareferencia, '$.dados.diagnostico_principal') as diagnostico_principal,
            json_extract_scalar(doc_contrareferencia, '$.dados.encaminhamento') as encaminhamento,

            json_extract_scalar(doc_contrareferencia, '$.dados.problema') as problema,
            json_extract_scalar(doc_contrareferencia, '$.dados.motivo_coicide') as motivo_coicide,

            json_extract_scalar(doc_contrareferencia, '$.profissional.nome') as profissional_nome,
            json_extract_scalar(doc_contrareferencia, '$.profissional.cpf') as profissional_cpf,
            json_extract_scalar(doc_contrareferencia, '$.profissional.cns') as profissional_cns,
            json_extract_scalar(doc_contrareferencia, '$.profissional.cbo') as profissional_cbo

        from contrareferencia_filtrado
    ),

    cast_rename as (
        select
            safe_cast({{ process_null('source_id') }} as string) as source_id,
            safe_cast({{ process_null('id_cnes') }} as string) as id_cnes,
            safe_cast({{ process_null('unidade_nome') }} as string) as unidade_nome,
            safe_cast({{ process_null('unidade_municipio') }} as string) as unidade_municipio,
            safe_cast({{ process_null('id_contrareferencia') }} as string) as id_documento,
            safe_cast({{ process_null('contraferencia_numero') }} as string) as contraferencia_numero,
            safe_cast({{ process_null('contrareferencia_datahora') }} as datetime) as contrareferencia_datahora,

            safe_cast({{ process_null('motivo') }} as string) as motivo,
            safe_cast({{ process_null('resultados') }} as string) as resultados,
            safe_cast({{ process_null('conduta') }} as string) as conduta,
            safe_cast({{ process_null('impressao') }} as string) as impressao,
            safe_cast({{ process_null('conduta_seguimento') }} as string) as conduta_seguimento,
            safe_cast({{ process_null('resumo') }} as string) as resumo,

            safe_cast({{ process_null('cid_principal') }} as string) as cid_principal,
            safe_cast({{ process_null('diagnostico_principal') }} as string) as diagnostico_principal,
            safe_cast({{ process_null('encaminhamento') }} as string) as encaminhamento,

            safe_cast({{ process_null('problema') }} as boolean) as flag_problema,
            safe_cast({{ process_null('motivo_coicide') }} as boolean) as flag_motivo_coincide,

            safe_cast({{ process_null('profissional_nome') }} as string) as profissional_nome,
            safe_cast({{ process_null('profissional_cpf') }} as string) as profissional_cpf,
            safe_cast({{ process_null('profissional_cns') }} as string) as profissional_cns,
            safe_cast({{ process_null('profissional_cbo') }} as string) as profissional_cbo
        from contrareferencia
    )

select * from cast_rename