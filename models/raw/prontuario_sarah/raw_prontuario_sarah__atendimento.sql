{{
    config(
        alias="atendimento",
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

    atendimento_filtrado as (
        select 
            source_id,
            doc as doc_atendimento
        from source,
        unnest(json_extract_array(data, '$.documentos')) AS doc
        where json_value(doc, '$.tipo') = 'ATENDIMENTO MÉDICO'
    ),

    atendimento as (
        select
            s.source_id,
            json_extract_scalar(data,'$.cnes') as id_cnes,
            json_extract_scalar(data,'$.atendimento.numero') as atendimento_numero,
            json_extract_scalar(data,'$.atendimento.tipo') as atendimento_tipo,
            json_extract_scalar(data,'$.atendimento.subtipo') as atendimento_subtipo,
            json_extract_scalar(data,'$.atendimento.datahora_entrada') as datahora_entrada,
            json_extract_scalar(data,'$.atendimento.datahora_saida') as datahora_saida,

            json_extract_scalar(data,'$.paciente.cpf') as paciente_cpf,
            json_extract_scalar(data,'$.paciente.nome_social') as paciente_nome_social,
            json_extract_scalar(data,'$.paciente.nome') as paciente_nome,
            json_extract_scalar(data,'$.paciente.cns') as paciente_cns,
            json_extract_scalar(data,'$.paciente.data_nascimento') as paciente_data_nascimento,
            json_extract_scalar(data,'$.paciente.fone') as paciente_telefone,
            json_extract_scalar(data,'$.paciente.municipio_naturalidade') as paciente_municipio_naturalidade,
            json_extract_scalar(data,'$.paciente.municipio_residencia') as paciente_municipio_residencia,
            json_extract_scalar(data,'$.paciente.alergias') as paciente_alergias,


            json_extract_scalar(am.doc_atendimento, '$.id') as id_atendimento_medico,
            json_extract_scalar(am.doc_atendimento, '$.numero') as atendimento_medico_numero,
            json_extract_scalar(am.doc_atendimento, '$.datahora') as atendimento_medico_datahora,

            {{ base64_to_string("json_extract_scalar(am.doc_atendimento, '$.dados.historia_doenca_atual')") }} as historia_doenca_atual,
            {{ base64_to_string("json_extract_scalar(am.doc_atendimento, '$.dados.hipotese_diagnostica')") }} as hipotese_diagnostica,
            {{ base64_to_string("json_extract_scalar(am.doc_atendimento, '$.dados.conduta_imediata')") }} as conduta_imediata,
            {{ base64_to_string("json_extract_scalar(am.doc_atendimento, '$.dados.exame_fisico')") }} as exame_fisico,
            {{ base64_to_string("json_extract_scalar(am.doc_atendimento, '$.dados.medicamentos_em_uso')") }} as medicamentos_em_uso,
            {{ base64_to_string("json_extract_scalar(am.doc_atendimento, '$.dados.historia_pessoal_pregressa')") }} as historia_pessoal_pregressa,
            {{ base64_to_string("json_extract_scalar(am.doc_atendimento, '$.dados.historia_familiar')") }} as historia_familiar,
            {{ base64_to_string("json_extract_scalar(am.doc_atendimento, '$.dados.historia_social')") }} as historia_social,

            json_extract_scalar(am.doc_atendimento, '$.dados.cid_principal') as cid_principal,
            json_extract_scalar(am.doc_atendimento, '$.dados.cid_secundario') as cid_secundario,
            json_extract_scalar(am.doc_atendimento, '$.dados.encaminhamento') as encaminhamento,
            json_extract_scalar(am.doc_atendimento, '$.dados.dias_retorno') as dias_retorno,

            json_extract_scalar(am.doc_atendimento, '$.profissional.nome') as profissional_nome,
            json_extract_scalar(am.doc_atendimento, '$.profissional.cpf') as profissional_cpf,
            json_extract_scalar(am.doc_atendimento, '$.profissional.cns') as profissional_cns,
            json_extract_scalar(am.doc_atendimento, '$.profissional.cbo') as profissional_cbo,

            json_extract_scalar(am.doc_atendimento, '$.procedimento.tabela') as procedimento_tabela,
            json_extract_scalar(am.doc_atendimento, '$.procedimento.codigo') as procedimento_codigo

        from source s
        left join atendimento_filtrado am on s.source_id = am.source_id

    ),

    cast_rename as (
        select 
            safe_cast({{ process_null('source_id') }} as string) as source_id,
            safe_cast({{ process_null('id_cnes') }} as string) as id_cnes,
            safe_cast({{ process_null('atendimento_numero') }} as string) as atendimento_numero,
            safe_cast({{ process_null('atendimento_tipo') }} as string) as atendimento_tipo,
            safe_cast({{ process_null('atendimento_subtipo') }} as string) as atendimento_subtipo,
            safe_cast({{ process_null('datahora_entrada') }} as datetime) as datahora_entrada,
            safe_cast({{ process_null('datahora_saida') }} as datetime) as datahora_saida,

            safe_cast({{ process_null('paciente_cpf') }} as string) as paciente_cpf,
            safe_cast({{ process_null('paciente_nome_social') }} as string) as paciente_nome_social,
            safe_cast({{ process_null('paciente_nome') }} as string) as paciente_nome,
            safe_cast({{ process_null('paciente_cns') }} as string) as paciente_cns,
            safe_cast({{ process_null('paciente_data_nascimento') }} as date) as paciente_data_nascimento,
            safe_cast({{ process_null('paciente_telefone') }} as string) as paciente_telefone,
            safe_cast({{ process_null('paciente_municipio_naturalidade') }} as string) as paciente_municipio_naturalidade,
            safe_cast({{ process_null('paciente_municipio_residencia') }} as string) as paciente_municipio_residencia,
            safe_cast({{ process_null('paciente_alergias') }} as string) as paciente_alergias,

            safe_cast({{ process_null('id_atendimento_medico') }} as string) as id_atendimento_medico,
            safe_cast({{ process_null('atendimento_medico_numero') }} as string) as atendimento_medico_numero,
            safe_cast({{ process_null('atendimento_medico_datahora') }} as datetime) as atendimento_medico_datahora,    

            safe_cast({{ process_null('historia_doenca_atual') }} as string) as historia_doenca_atual,
            safe_cast({{ process_null('hipotese_diagnostica') }} as string) as hipotese_diagnostica,
            safe_cast({{ process_null('conduta_imediata') }} as string) as conduta_imediata,
            safe_cast({{ process_null('exame_fisico') }} as string) as exame_fisico,
            safe_cast({{ process_null('medicamentos_em_uso') }} as string) as medicamentos_em_uso,
            safe_cast({{ process_null('historia_pessoal_pregressa') }} as string) as historia_pessoal_pregressa,
            safe_cast({{ process_null('historia_familiar') }} as string) as historia_familiar,
            safe_cast({{ process_null('historia_social') }} as string) as historia_social,

            safe_cast({{ process_null('cid_principal') }} as string) as cid_principal,
            safe_cast({{ process_null('cid_secundario') }} as string) as cid_secundario,
            safe_cast({{ process_null('encaminhamento') }} as string) as encaminhamento,
            safe_cast({{ process_null('dias_retorno') }} as int64) as dias_retorno,

            safe_cast({{ process_null('procedimento_tabela') }} as string) as procedimento_tabela,
            safe_cast({{ process_null('procedimento_codigo') }} as string) as procedimento_codigo,

            safe_cast({{ process_null('profissional_nome') }} as string) as profissional_nome,
            safe_cast({{ process_null('profissional_cpf') }} as string) as profissional_cpf,
            safe_cast({{ process_null('profissional_cns') }} as string) as profissional_cns,
            safe_cast({{ process_null('profissional_cbo') }} as string) as profissional_cbo
        from atendimento
    )

select 
    * 
from cast_rename
