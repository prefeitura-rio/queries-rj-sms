-- noqa: disable=LT08

{{
    config(
        enabled=true,
        schema="brutos_sisreg_api",
        alias="solicitacoes",
        partition_by={
            "field": "particao_data",
            "data_type": "date",
            "granularity": "month",
        },
    )
}}

with
    casted_partitions as (
        select 
            safe_cast(data_particao as date) as data_particao
        from {{ source("brutos_sisreg_api_staging", "solicitacoes") }}
    ),
    
    most_complete_partition as (
        select 
            data_particao, 
            count(*) as registros
        from casted_partitions
        group by data_particao
        order by registros desc
        limit 1
    ),

    source as (
        select
            -- Identificação básica da solicitação
            {{ process_null("codigo_solicitacao") }} as solicitacao_id,
            safe_cast({{ process_null("data_solicitacao") }} as timestamp) as data_solicitacao,
            safe_cast({{ process_null("data_atualizacao") }} as timestamp) as data_atualizacao,
            safe_cast({{ process_null("data_cancelamento") }} as timestamp) as data_cancelamento,

            -- Status e classificação
            {{ process_null("status_solicitacao") }} as solicitacao_status,
            {{ process_null("sigla_situacao") }} as solicitacao_situacao,
            case
                when st_visualizado_regulador = "1" then "SIM"
                when st_visualizado_regulador = "0" then "NÃO"
                else null
            end as solicitacao_visualizada_regulador,
            {{ process_null("codigo_tipo_regulacao") }} as regulacao_tp_id,
            {{ process_null("codigo_tipo_fila") }} as fila_tp_id,  -- TODO: implementar de/para
            split({{ process_null("codigo_perfil_cancelamento") }}, '.')[offset(0)] as perfil_cancelamento_id,  -- TODO: implementar de/para
            case
                when codigo_classificacao_risco = "1" then 'VERMELHO'
                when codigo_classificacao_risco = "2" then 'AMARELO'
                when codigo_classificacao_risco = "3" then 'VERDE'
                when codigo_classificacao_risco = "4" then 'AZUL'
                else null
            end as solicitacao_risco,

            -- Informações da solicitação
            lpad({{ process_null("codigo_grupo_procedimento") }}, 7, '0') as procedimento_grupo_id,
            {{ clean_name_string(process_null("nome_grupo_procedimento")) }} as procedimento_grupo,
            case
                when {{ process_null("codigo_tipo_vaga_solicitada") }} = "1" then '1 VEZ'
                when {{ process_null("codigo_tipo_vaga_solicitada") }} = "2" then 'RETORNO'
                when {{ process_null("codigo_tipo_vaga_solicitada") }} = "3" then 'RESERVA TECNICA'
                when {{ process_null("codigo_tipo_vaga_solicitada") }} = "4" then 'SEM INFORMACAO'
                else null
            end as vaga_solicitada_tp,
            upper({{ process_null("codigo_cid_solicitado") }}) as cid_id,
            {{ clean_name_string(process_null("descricao_cid_solicitado")) }} as cid,

            -- Dados do procedimento
            json_value(proceds_json, '$.codigo_interno')     as procedimento_id,
            json_value(proceds_json, '$.descricao_interna')  as procedimento,
            json_value(proceds_json, '$.codigo_sigtap')      as procedimento_sigtap_id,
            json_value(proceds_json, '$.descricao_sigtap')   as procedimento_sigtap,

            -- Dados do solicitante
            {{ process_null("codigo_uf_solicitante") }} as uf_solicitante_id,
            {{ process_null("sigla_uf_solicitante") }} as uf_solicitante,
            lpad({{ process_null("codigo_cnes_central_solicitante") }}, 7, '0') as central_solicitante_id_cnes,
            {{ process_null("codigo_central_solicitante") }} as central_solicitante_id,
            {{ clean_name_string(process_null("nome_cnes_central_solicitante")) }} as central_solicitante_cnes,
            {{ clean_name_string(process_null("nome_central_solicitante")) }} as central_solicitante,
            lpad({{ process_null("codigo_unidade_solicitante") }}, 7, '0') as unidade_solicitante_id,
            {{ clean_name_string(process_null("nome_unidade_solicitante")) }} as unidade_solicitante,
            lpad({{ process_null("cpf_profissional_solicitante") }}, 11, '0') as profissional_solicitante_cpf,
            {{ clean_name_string(process_null("nome_medico_solicitante")) }} as medico_solicitante,

            -- Dados dos operadores
            {{ clean_name_string(process_null("nome_operador_solicitante")) }} as operador_solicitante_nome,
            {{ clean_name_string(process_null("nome_operador_cancelamento")) }} as operador_cancelamento_nome,
            {{ clean_name_string(process_null("nome_operador_videofonista")) }} as operador_videofonista_nome,

            -- Dados do regulador
            {{ process_null("codigo_uf_regulador") }} as uf_regulador_id,
            {{ clean_name_string(process_null("sigla_uf_regulador")) }} as uf_regulador,
            {{ process_null("codigo_central_reguladora") }} as central_reguladora_id,
            {{ clean_name_string(process_null("nome_central_reguladora")) }} as central_reguladora,
            {{ process_null("nome_perfil_cancelamento") }} as perfil_cancelamento,

            -- Dados do executante
            {{ process_null("numero_crm") }} as crm,

            -- Preferências da solicitação
            safe_cast({{ process_null("data_desejada") }} as timestamp) as data_desejada,
            lpad({{ process_null("codigo_unidade_desejada") }}, 7, '0') as unidade_desejada_id,
            {{ clean_name_string(process_null("nome_unidade_desejada")) }} as unidade_desejada,

            -- Dados do paciente
            lpad({{ process_null("cpf_usuario") }}, 11, '0') as paciente_cpf,
            lpad({{ process_null("cns_usuario") }}, 15, '0') as paciente_cns,
            {{ clean_name_string(process_null("no_usuario")) }} as paciente_nome,
            safe_cast({{ process_null("dt_nascimento_usuario") }} as timestamp) as paciente_dt_nasc,
            {{ clean_name_string(process_null("sexo_usuario")) }} as paciente_sexo,
            {{ clean_name_string(process_null("no_mae_usuario")) }} as paciente_nome_mae,
            {{ process_null("telefone") }} as paciente_telefone,

            -- Endereço do paciente
            {{ clean_name_string(process_null("nome_municipio_nascimento")) }} as paciente_mun_nasc,
            {{ process_null("uf_municipio_nascimento") }} as paciente_uf_nasc,
            {{ clean_name_string(process_null("uf_paciente_residencia")) }} as paciente_uf_res,
            {{ clean_name_string(process_null("municipio_paciente_residencia")) }} as paciente_mun_res,
            {{ clean_name_string(process_null("bairro_paciente_residencia")) }} as paciente_bairro_res,
            lpad({{ process_null("cep_paciente_residencia") }}, 8, '0') as paciente_cep_res,
            {{ clean_name_string(process_null("endereco_paciente_residencia")) }} as paciente_endereco_res,
            {{ clean_name_string(process_null("complemento_paciente_residencia")) }} as paciente_complemento_res,
            {{ process_null("numero_paciente_residencia") }} as paciente_numero_res,
            {{ process_null("tipo_logradouro_paciente_residencia") }} as paciente_tp_logradouro_res,

            -- Laudo
            json_value(laudo_json, '$.codigo_cnes_operador') as laudo_operador_cnes_id,
            json_value(laudo_json, '$.nome_cnes_operador') as laudo_operador_cnes,
            json_value(laudo_json, '$.operador') as laudo_operador,
            json_value(laudo_json, '$.tipo_perfil') as laudo_perfil_tp,
            json_value(laudo_json, '$.tipo_descricao') as laudo_descricao_tp,
            json_value(laudo_json, '$.situacao') as laudo_situacao,
            json_value(laudo_json, '$.observacao') as laudo_observacao,
            safe_cast(json_value(laudo_json, '$.data_observacao') as timestamp) as laudo_data_observacao,

            -- Metadados Elasticsearch
            type as elastic__type,
            carga_epoch as elastic__carga_epoch,
            timestamp as elastic__timestamp,
            version as elastic__version,

            -- Metadado SMS 
            safe_cast(safe_cast({{ process_null("data_extracao")}}  as timestamp) as date) as data_extracao,

            -- Partições
            safe_cast(ano_particao as int) as particao_ano,
            safe_cast(mes_particao as int) as particao_mes,
            safe_cast(data_particao as date) as particao_data


        from {{ source("brutos_sisreg_api_staging", "solicitacoes") }}
        left join unnest(json_extract_array(replace(laudo, "'", '"'))) as laudo_json
        left join unnest(json_extract_array(replace(procedimentos, "'", '"'))) as proceds_json
        where safe_cast(data_particao as date) = (select data_particao from most_complete_partition)

    )

select * from source
