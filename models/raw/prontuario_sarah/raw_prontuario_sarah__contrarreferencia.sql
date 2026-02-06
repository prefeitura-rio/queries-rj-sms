{{
    config(
        alias="contrarreferencia",
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

    contrarreferencia_filtrado as (
        select
            source_id,

            json_extract_scalar(data, "$.paciente.nome") as paciente_nome,
            json_extract_scalar(data, "$.paciente.nome_social") as paciente_nome_social,
            json_extract_scalar(data, "$.paciente.cpf") as paciente_cpf,
            json_extract_scalar(data, "$.paciente.cns") as paciente_cns,
            json_extract_scalar(data, "$.paciente.data_nascimento") as paciente_data_nascimento,
            json_extract_scalar(data, "$.paciente.fone") as paciente_telefone,
            -- Municípios são códigos do IBGE, ex. "3304557" = Rio de Janeiro
            json_extract_scalar(data, "$.paciente.municipio_naturalidade") as paciente_cod_municipio_naturalidade,
            json_extract_scalar(data, "$.paciente.municipio_residencia") as paciente_cod_municipio_residencia,

            doc as doc_cr
        from source,
        unnest(json_extract_array(data, '$.documentos')) as doc
        where json_value(doc, '$.tipo') in (
            'CONTRAREFERÊNCIA',  -- Atualmente mandam assim
            'CONTRARREFERÊNCIA'  -- Por precaução caso um dia consertem
        )
    ),

    contrarreferencia as (
        select
            * except(doc_cr),

            json_extract_scalar(doc_cr, '$.dados.unidade.cnes') as id_cnes,
            json_extract_scalar(doc_cr, '$.dados.unidade.nome') as unidade_nome,
            json_extract_scalar(doc_cr, '$.dados.unidade.municipio') as unidade_cod_municipio,  -- Código IBGE

            json_extract_scalar(doc_cr, '$.id') as id_contrarreferencia,
            json_extract_scalar(doc_cr, '$.numero') as contrarreferencia_numero,
            json_extract_scalar(doc_cr, '$.datahora') as contrarreferencia_datahora,

            {{ base64_to_string("json_extract_scalar(doc_cr, '$.dados.motivo')") }} as motivo,
            {{ base64_to_string("json_extract_scalar(doc_cr, '$.dados.resultados')") }} as resultados,
            {{ base64_to_string("json_extract_scalar(doc_cr, '$.dados.conduta')") }} as conduta,
            {{ base64_to_string("json_extract_scalar(doc_cr, '$.dados.impressao')") }} as impressao,
            {{ base64_to_string("json_extract_scalar(doc_cr, '$.dados.conduta_seguimento')") }} as conduta_seguimento,
            {{ base64_to_string("json_extract_scalar(doc_cr, '$.dados.resumo')") }} as resumo,

            json_extract_scalar(doc_cr, '$.dados.cid_principal') as cid_principal,
            json_extract_scalar(doc_cr, '$.dados.diagnostico_principal') as diagnostico_principal,
            json_extract_scalar(doc_cr, '$.dados.encaminhamento') as encaminhamento,

            json_extract_scalar(doc_cr, '$.dados.problema') as problema,
            json_extract_scalar(doc_cr, '$.dados.motivo_coicide') as motivo_coicide,

            json_extract_scalar(doc_cr, '$.profissional.nome') as profissional_nome,
            json_extract_scalar(doc_cr, '$.profissional.cpf') as profissional_cpf,
            json_extract_scalar(doc_cr, '$.profissional.cns') as profissional_cns,
            json_extract_scalar(doc_cr, '$.profissional.cbo') as profissional_cbo

        from contrarreferencia_filtrado
    ),

    cast_rename as (
        select
            safe_cast({{ process_null("paciente_nome") }} as string) as paciente_nome,
            safe_cast({{ process_null("paciente_nome_social") }} as string) as paciente_nome_social,
            safe_cast({{ process_null("paciente_cpf") }} as string) as paciente_cpf,
            safe_cast({{ process_null("paciente_cns") }} as string) as paciente_cns,
            safe_cast({{ process_null("paciente_data_nascimento") }} as string) as paciente_data_nascimento,
            safe_cast({{ process_null("paciente_telefone") }} as string) as paciente_telefone,
            safe_cast({{ process_null("paciente_cod_municipio_naturalidade") }} as string) as paciente_cod_municipio_naturalidade,
            safe_cast({{ process_null("paciente_cod_municipio_residencia") }} as string) as paciente_cod_municipio_residencia,

            safe_cast({{ process_null('source_id') }} as string) as source_id,
            safe_cast({{ process_null('id_cnes') }} as string) as id_cnes,
            safe_cast({{ process_null('unidade_nome') }} as string) as unidade_nome,
            safe_cast({{ process_null('unidade_cod_municipio') }} as string) as unidade_cod_municipio,
            safe_cast({{ process_null('id_contrarreferencia') }} as string) as id_documento,
            safe_cast({{ process_null('contrarreferencia_numero') }} as string) as contrarreferencia_numero,
            safe_cast({{ process_null('contrarreferencia_datahora') }} as datetime) as contrarreferencia_datahora,

            safe_cast({{ process_null('trim(motivo)') }} as string) as motivo,
            safe_cast({{ process_null('trim(resultados)') }} as string) as resultados,
            safe_cast({{ process_null('trim(conduta)') }} as string) as conduta,
            safe_cast({{ process_null('trim(impressao)') }} as string) as impressao,
            safe_cast({{ process_null('trim(conduta_seguimento)') }} as string) as conduta_seguimento,
            safe_cast({{ process_null('trim(resumo)') }} as string) as resumo,

            safe_cast({{ process_null('cid_principal') }} as string) as cid_principal,
            safe_cast({{ process_null('diagnostico_principal') }} as string) as diagnostico_principal,
            safe_cast({{ process_null('encaminhamento') }} as string) as encaminhamento,

            safe_cast({{ process_null('problema') }} as boolean) as flag_problema,
            safe_cast({{ process_null('motivo_coicide') }} as boolean) as flag_motivo_coincide,

            safe_cast({{ process_null('profissional_nome') }} as string) as profissional_nome,
            safe_cast({{ process_null('profissional_cpf') }} as string) as profissional_cpf,
            safe_cast({{ process_null('profissional_cns') }} as string) as profissional_cns,
            safe_cast({{ process_null('profissional_cbo') }} as string) as profissional_cbo
        from contrarreferencia
    )

select * from cast_rename
