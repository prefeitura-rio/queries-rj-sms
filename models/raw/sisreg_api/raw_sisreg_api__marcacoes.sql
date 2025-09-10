-- models/brutos_sisreg_api/marcacoes.sql
-- noqa: disable=LT08
{{
  config(
    enabled = true,
    materialized = 'incremental',
    schema = "brutos_sisreg_api",
    alias  = "marcacoes",

    incremental_strategy = 'insert_overwrite',
    partition_by = {
      "field": "data_atualizacao",
      "data_type": "date",
      "granularity": "month"
    },

    cluster_by = ['unidade_solicitante_id','unidade_executante_id','procedimento_interno_id'],
    on_schema_change = 'sync_all_columns'
  )
}}

{% set months_lookback = var('months_lookback', 3) %}

with
  latest_src_partition as (
    select max(cast(data_particao as date)) as latest_load_dt
    from {{ ref('raw_sisreg_api_log__logs') }}
    where bq_table = 'marcacoes'
  ),

  sisreg as (
    select s.*
    from {{ source('brutos_sisreg_api_staging', 'marcacoes') }} s
    where cast(s.data_particao as date) = (select latest_load_dt from latest_src_partition)
  ),

    sisreg_transformed as (
        select
            -- Metadados da extração
            run_id,

            -- Identificação básica da solicitação
            {{ process_null("codigo_solicitacao") }} as solicitacao_id,
            safe_cast(
                {{ process_null("data_solicitacao") }} as timestamp
            ) as data_solicitacao,
            safe_cast(
                {{ process_null("data_atualizacao") }} as timestamp
            ) as data_atualizacao,
            safe_cast(
                {{ process_null("data_cancelamento") }} as timestamp
            ) as data_cancelamento,

            -- Status e classificação
            {{ process_null("status_solicitacao") }} as solicitacao_status,
            {{ process_null("sigla_situacao") }} as solicitacao_situacao,
            case
                when st_visualizado_regulador = "1"
                then "SIM"
                when st_visualizado_regulador = "0"
                then "NÃO"
                else null
            end as solicitacao_visualizada_regulador,
            {{ process_null("codigo_tipo_regulacao") }} as regulacao_tp_id,
            {{ process_null("codigo_tipo_fila") }} as fila_tp_id,  -- TODO: implementar de/para
            split({{ process_null("codigo_perfil_cancelamento") }}, '.')[
                offset(0)
            ] as perfil_cancelamento_id,  -- TODO: implementar de/para
            case
                when codigo_classificacao_risco = "1"
                then 'VERMELHO'
                when codigo_classificacao_risco = "2"
                then 'AMARELO'
                when codigo_classificacao_risco = "3"
                then 'VERDE'
                when codigo_classificacao_risco = "4"
                then 'AZUL'
                else null
            end as solicitacao_risco,
            {{ clean_name_string(process_null("justificativa_cancelamento")) }}
            as justificativa_cancelamento,
            {{ process_null("chave_confirmacao") }} as chave_confirmacao,

            -- Informações da solicitação
            lpad(
                {{ process_null("codigo_grupo_procedimento") }}, 7, '0'
            ) as procedimento_grupo_id,
            upper(trim({{ process_null("nome_grupo_procedimento") }}))
            as procedimento_grupo,
            case
                when {{ process_null("codigo_tipo_vaga_solicitada") }} = "1"
                then '1 VEZ'
                when {{ process_null("codigo_tipo_vaga_solicitada") }} = "2"
                then 'RETORNO'
                when {{ process_null("codigo_tipo_vaga_solicitada") }} = "3"
                then 'RESERVA TECNICA'
                when {{ process_null("codigo_tipo_vaga_solicitada") }} = "4"
                then 'SEM INFORMACAO'
                else null
            end as vaga_solicitada_tp,
            upper({{ process_null("codigo_cid_solicitado") }}) as cid_id,
            {{ clean_name_string(process_null("descricao_cid_solicitado")) }} as cid,

            -- Dados do procedimento
            lpad(
                {{ process_null("codigo_interno_procedimento") }}, 7, '0'
            ) as procedimento_interno_id,
            upper(trim({{ process_null("descricao_interna_procedimento") }}))
            as procedimento_interno,
            lpad(
                {{ process_null("codigo_sigtap_procedimento") }}, 10, '0'
            ) as procedimento_sigtap_id,
            {{ clean_name_string(process_null("descricao_sigtap_procedimento")) }}
            as procedimento_sigtap,

            -- Dados do solicitante
            {{ process_null("codigo_uf_solicitante") }} as uf_solicitante_id,
            {{ process_null("sigla_uf_solicitante") }} as uf_solicitante,
            lpad(
                {{ process_null("codigo_cnes_central_solicitante") }}, 7, '0'
            ) as central_solicitante_id_cnes,
            {{ process_null("codigo_central_solicitante") }} as central_solicitante_id,
            {{ clean_name_string(process_null("nome_cnes_central_solicitante")) }}
            as central_solicitante_cnes,
            {{ clean_name_string(process_null("nome_central_solicitante")) }}
            as central_solicitante,
            lpad(
                {{ process_null("codigo_unidade_solicitante") }}, 7, '0'
            ) as unidade_solicitante_id,
            upper(trim({{ process_null("nome_unidade_solicitante") }}))
            as unidade_solicitante,
            lpad(
                {{ process_null("cpf_profissional_solicitante") }}, 11, '0'
            ) as profissional_solicitante_cpf,
            {{ clean_name_string(process_null("nome_medico_solicitante")) }}
            as medico_solicitante,
            {{ process_null("numero_crm") }} as crm_solicitante,

            -- Dados dos operadores
            {{ clean_name_string(process_null("nome_operador_solicitante")) }}
            as operador_solicitante_nome,
            {{ clean_name_string(process_null("nome_operador_cancelamento")) }}
            as operador_cancelamento_nome,
            {{ clean_name_string(process_null("nome_operador_videofonista")) }}
            as operador_videofonista_nome,

            -- Dados do regulador
            {{ process_null("codigo_uf_regulador") }} as uf_regulador_id,
            {{ clean_name_string(process_null("sigla_uf_regulador")) }} as uf_regulador,
            {{ process_null("codigo_central_reguladora") }} as central_reguladora_id,
            {{ clean_name_string(process_null("nome_central_reguladora")) }}
            as central_reguladora,
            {{ process_null("nome_perfil_cancelamento") }} as perfil_cancelamento,

            -- Preferências da solicitação
            safe_cast(
                {{ process_null("data_desejada") }} as timestamp
            ) as data_desejada,
            lpad(
                {{ process_null("codigo_unidade_desejada") }}, 7, '0'
            ) as unidade_desejada_id,
            {{ clean_name_string(process_null("nome_unidade_desejada")) }}
            as unidade_desejada,

            -- Dados do paciente
            lpad({{ process_null("cpf_usuario") }}, 11, '0') as paciente_cpf,
            lpad({{ process_null("cns_usuario") }}, 15, '0') as paciente_cns,
            {{ clean_name_string(process_null("no_usuario")) }} as paciente_nome,
            safe_cast(
                {{ process_null("dt_nascimento_usuario") }} as timestamp
            ) as paciente_dt_nasc,
            {{ clean_name_string(process_null("sexo_usuario")) }} as paciente_sexo,
            {{ clean_name_string(process_null("no_mae_usuario")) }}
            as paciente_nome_mae,
            {{ process_null("telefone") }} as paciente_telefone,

            -- Endereço do paciente
            {{ clean_name_string(process_null("nome_municipio_nascimento")) }}
            as paciente_mun_nasc,
            {{ process_null("uf_municipio_nascimento") }} as paciente_uf_nasc,
            {{ clean_name_string(process_null("uf_paciente_residencia")) }}
            as paciente_uf_res,
            {{ clean_name_string(process_null("municipio_paciente_residencia")) }}
            as paciente_mun_res,
            {{ clean_name_string(process_null("bairro_paciente_residencia")) }}
            as paciente_bairro_res,
            lpad(
                {{ process_null("cep_paciente_residencia") }}, 8, '0'
            ) as paciente_cep_res,
            {{ clean_name_string(process_null("endereco_paciente_residencia")) }}
            as paciente_endereco_res,
            {{ clean_name_string(process_null("complemento_paciente_residencia")) }}
            as paciente_complemento_res,
            {{ process_null("numero_paciente_residencia") }} as paciente_numero_res,
            {{ process_null("tipo_logradouro_paciente_residencia") }}
            as paciente_tp_logradouro_res,

            -- Laudo
            json_value(laudo_json, '$.codigo_cnes_operador') as laudo_operador_cnes_id,
            json_value(laudo_json, '$.tipo_descricao') as laudo_descricao_tp,
            json_value(laudo_json, '$.situacao') as laudo_situacao,
            json_value(laudo_json, '$.observacao') as laudo_observacao,
            safe_cast(
                json_value(laudo_json, '$.data_observacao') as timestamp
            ) as laudo_data_observacao,

            -- Dados do operador autorizador
            {{ clean_name_string(process_null("nome_operador_autorizador")) }}
            as operador_autorizador_nome,
            {{ process_null("codigo_perfil_operador_autorizador") }}
            as operador_autorizador_perfil_id,
            {{ clean_name_string(process_null("nome_perfil_operador_autorizador")) }}
            as operador_autorizador_perfil,

            -- Dados da central executante
            lpad(
                {{ process_null("codigo_cnes_central_executante") }}, 7, '0'
            ) as central_executante_id_cnes,
            {{ clean_name_string(process_null("nome_cnes_central_executante")) }}
            as central_executante_cnes,

            -- Dados da unidade executante
            upper(trim({{ process_null("nome_unidade_executante") }}))
            as unidade_executante_nome,
            lpad(
                {{ process_null("codigo_unidade_executante") }}, 7, '0'
            ) as unidade_executante_id,
            {{ clean_name_string(process_null("logradouro_unidade_executante")) }}
            as unidade_executante_logradouro,
            {{ process_null("complemento_unidade_executante") }}
            as unidade_executante_complemento,
            {{ process_null("numero_unidade_executante") }}
            as unidade_executante_numero,
            {{ clean_name_string(process_null("bairro_unidade_executante")) }}
            as unidade_executante_bairro,
            {{ clean_name_string(process_null("municipio_unidade_executante")) }}
            as unidade_executante_municipio,
            {{ clean_name_string(process_null("cep_unidade_executante")) }}
            as unidade_executante_cep,
            {{ process_null("telefone_unidade_executante") }}
            as unidade_executante_telefone,

            -- Dados do profissional executante
            lpad(
                {{ process_null("cpf_profissional_executante") }}, 11, '0'
            ) as profissional_executante_cpf,
            {{ clean_name_string(process_null("nome_profissional_executante")) }}
            as profissional_executante_nome,

            -- Dados da marcação
            {{ process_null("codigo_marcacao") }} as marcacao_id,
            safe_cast(
                {{ process_null("data_marcacao") }} as timestamp
            ) as data_marcacao,
            safe_cast(
                {{ process_null("data_aprovacao") }} as timestamp
            ) as data_aprovacao,
            safe_cast(
                {{ process_null("data_confirmacao") }} as timestamp
            ) as data_confirmacao,
            {{ process_null("marcacao_executada") }} as marcacao_executada,
            case
                when {{ process_null("st_falta_registrada") }} = "1"
                then "SIM"
                when {{ process_null("st_falta_registrada") }} = "0"
                then "NÃO"
                else null
            end as falta_registrada,
            case
                when {{ process_null("st_paciente_avisado") }} = "1"
                then "SIM"
                when {{ process_null("st_paciente_avisado") }} = "0"
                then "NÃO"
                else null
            end as paciente_avisado,
            case
                when {{ process_null("codigo_tipo_vaga_consumida") }} = "1"
                then '1 VEZ'
                when {{ process_null("codigo_tipo_vaga_consumida") }} = "2"
                then 'RETORNO'
                when {{ process_null("codigo_tipo_vaga_consumida") }} = "3"
                then 'RESERVA TECNICA'
                when {{ process_null("codigo_tipo_vaga_consumida") }} = "4"
                then 'SEM INFORMACAO'
                else null
            end as vaga_consumida_tp,
            upper({{ process_null("codigo_cid_agendado") }}) as cid_agendado_id,
            {{ clean_name_string(process_null("descricao_cid_agendado")) }}
            as cid_agendado,

            -- Metadado SMS 
            safe_cast(safe_cast({{ process_null("data_extracao")}}  as timestamp) as date) as data_extracao,

            -- Partições
            cast(ano_particao as int) as particao_ano,
            cast(mes_particao as int) as particao_mes,            
            parse_date('%Y-%m-%d', data_particao) as particao_data

        from sisreg
        left join unnest(json_extract_array(replace(laudo, "'", '"'))) as laudo_json
),

  windowed as (
    select *
    from sisreg_transformed
    where cast(data_atualizacao as date) between 
        date_sub(current_date(), interval {{ months_lookback }} month)
        and date_add(current_date(), interval 1 day)
)

select *
from windowed
qualify row_number() over (
  partition by solicitacao_id
  order by data_atualizacao desc nulls last
) = 1
