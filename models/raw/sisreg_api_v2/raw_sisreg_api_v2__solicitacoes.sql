{{
  config(
    schema = "brutos_sisreg_api_v2",
    alias  = "solicitacoes",
    partition_by = {
      "field": "data_particao",
      "data_type": "date",
      "granularity": "month"
    },
  )
}}


with
  dedup_source as (
    select *
    from {{ source("brutos_sisreg_api_v2_staging", "solicitacoes") }} s
    qualify row_number() over (
      partition by codigo_solicitacao
      order by _extracted_at desc nulls last
    ) = 1
  ),

  unnested as (
    select
      src.* except(laudo, procedimentos),
      ld as laudo,
      pd as procedimento
    from dedup_source as src,
      unnest(json_query_array(laudo)) as ld,
      unnest(json_query_array(procedimentos)) as pd
  ),

  sisreg as (
    select
      -- Identificação básica da solicitação
      cast({{ process_null("codigo_solicitacao") }} as string) as codigo_solicitacao,
      cast({{ process_null("data_solicitacao") }} as timestamp)  as datahora_solicitacao,
      cast({{ process_null("data_atualizacao") }} as timestamp)  as datahora_atualizacao,
      cast({{ process_null("data_cancelamento") }} as timestamp) as datahora_cancelamento,


      -- Status e classificação
      cast({{ process_null("status_solicitacao") }} as string) as status_solicitacao,
      cast({{ process_null("sigla_situacao") }} as string)     as sigla_situacao,
      case st_visualizado_regulador
          when "1" then "sim"
          when "0" then "nao"
          else cast(null as string)
      end as solicitacao_visualizada_regulador,

      -- TODO: o que cada código significa?
      cast({{ process_null("codigo_tipo_regulacao") }} as string)      as codigo_tipo_regulacao,
      cast({{ process_null("codigo_tipo_fila") }} as string)           as codigo_tipo_fila,
      cast({{ process_null("codigo_perfil_cancelamento") }} as string) as codigo_perfil_cancelamento,
      cast({{ process_null("codigo_classificacao_risco") }} as string) as codigo_classificacao_risco,
      case codigo_classificacao_risco
          when "1" then "vermelho"
          when "2" then "amarelo"
          when "3" then "verde"
          when "4" then "azul"
          else cast(null as string)
      end as classificacao_risco,


      -- Informações da solicitação
      lpad({{ process_null("codigo_grupo_procedimento") }}, 7, "0") as procedimento_grupo_codigo,
      trim({{ process_null("nome_grupo_procedimento") }}) as procedimento_grupo_nome,
      case {{ process_null("codigo_tipo_vaga_solicitada") }}
          when "1" then "1a vez"
          when "2" then "retorno"
          when "3" then "reserva tecnica"
          when "4" then "sem informacao"
          else cast(null as string)
      end as tipo_vaga_solicitada,
      upper({{ process_null("codigo_cid_solicitado") }}) as cid_id,
      cast({{ process_null("descricao_cid_solicitado") }} as string) as cid_descricao,


      -- Dados do procedimento
      json_value(procedimento, "$.codigo_interno")    as procedimento_id,
      json_value(procedimento, "$.descricao_interna") as procedimento_descricao,
      json_value(procedimento, "$.codigo_sigtap")     as procedimento_sigtap_id,
      json_value(procedimento, "$.descricao_sigtap")  as procedimento_sigtap_descricao,


      -- Dados do solicitante
      cast({{ process_null("codigo_uf_solicitante") }} as string) as uf_solicitante_codigo,
      cast({{ process_null("sigla_uf_solicitante") }} as string) as uf_solicitante_sigla,

      lpad({{ process_null("codigo_cnes_central_solicitante") }}, 7, "0") as central_solicitante_id_cnes,
      cast({{ process_null("codigo_central_solicitante") }} as string) as central_solicitante_codigo,
      cast({{ process_null("nome_cnes_central_solicitante") }} as string) as central_solicitante_nome_cnes,
      cast({{ process_null("nome_central_solicitante") }} as string) as central_solicitante_nome,

      lpad({{ process_null("codigo_unidade_solicitante") }}, 7, "0") as unidade_solicitante_codigo,
      trim({{ process_null("nome_unidade_solicitante") }}) as unidade_solicitante_nome,

      lpad({{ process_null("cpf_profissional_solicitante") }}, 11, "0") as profissional_solicitante_cpf,
      cast({{ process_null("nome_medico_solicitante") }} as string) as medico_solicitante_nome,


      -- Dados dos operadores
      cast({{ process_null("nome_operador_solicitante") }} as string) as operador_solicitante_nome,
      cast({{ process_null("nome_operador_cancelamento") }} as string) as operador_cancelamento_nome,
      cast({{ process_null("nome_operador_videofonista") }} as string) as operador_videofonista_nome,


      -- Dados do regulador
      cast({{ process_null("codigo_uf_regulador") }} as string) as uf_regulador_codigo,
      cast({{ process_null("sigla_uf_regulador") }} as string) as uf_regulador_sigla,

      cast({{ process_null("codigo_central_reguladora") }} as string) as central_reguladora_codigo,
      cast({{ process_null("nome_central_reguladora") }} as string) as central_reguladora_nome,

      cast({{ process_null("nome_perfil_cancelamento") }} as string) as perfil_cancelamento_nome,


      -- Dados do executante
      cast({{ process_null("numero_crm") }} as string) as crm,


      -- Preferências da solicitação
      cast({{ process_null("data_desejada") }} as timestamp) as data_desejada,
      lpad({{ process_null("codigo_unidade_desejada") }}, 7, "0") as unidade_desejada_codigo,
      cast({{ process_null("nome_unidade_desejada") }} as string) as unidade_desejada_nome,


      -- Dados do paciente
      lpad({{ process_null("cpf_usuario") }}, 11, "0") as paciente_cpf,
      lpad({{ process_null("cns_usuario") }}, 15, "0") as paciente_cns,
      cast({{ process_null("no_usuario") }} as string) as paciente_nome,
      cast({{ process_null("sexo_usuario") }} as string) as paciente_sexo,
      cast({{ process_null("no_mae_usuario") }} as string) as paciente_nome_mae,
      cast({{ process_null("telefone") }} as string) as paciente_telefones,
      -- -- Nascimento
      date({{ process_null("dt_nascimento_usuario") }}) as paciente_nascimento_data,
      cast({{ process_null("uf_municipio_nascimento") }} as string) as paciente_nascimento_uf,
      cast({{ process_null("nome_municipio_nascimento") }} as string) as paciente_nascimento_municipio,
      -- -- Residência
      cast({{ process_null("uf_paciente_residencia") }} as string) as paciente_residencia_uf,
      cast({{ process_null("municipio_paciente_residencia") }} as string) as paciente_residencia_municipio,
      cast({{ process_null("bairro_paciente_residencia") }} as string) as paciente_residencia_bairro,
      cast({{ process_null("endereco_paciente_residencia") }} as string) as paciente_residencia_endereco,
      cast({{ process_null("complemento_paciente_residencia") }} as string) as paciente_residencia_complemento,
      cast({{ process_null("numero_paciente_residencia") }} as string) as paciente_residencia_numero,
      cast({{ process_null("tipo_logradouro_paciente_residencia") }} as string) as paciente_residencia_logradouro,
      lpad({{ process_null("cep_paciente_residencia") }}, 8, "0") as paciente_residencia_cep,


      -- Laudo
      json_value(laudo, "$.codigo_cnes_operador") as laudo_operador_id_cnes,
      json_value(laudo, "$.nome_cnes_operador") as laudo_operador_unidade_nome,
      json_value(laudo, "$.operador") as laudo_operador,
      json_value(laudo, "$.tipo_perfil") as laudo_perfil_tipo,
      json_value(laudo, "$.tipo_descricao") as laudo_descricao_tipo,
      json_value(laudo, "$.situacao") as laudo_situacao,
      json_value(laudo, "$.observacao") as laudo_observacao,
      cast(json_value(laudo, "$.data_observacao") as timestamp) as laudo_datahora_observacao,
  
      -- Campos deixados de fora:
      --   'carga_epoch': '1779xxxxxx',
      --   'version': '1',
      --   'type': 'solicitacao-ambulatorial-atualizacao',
      --   'timestamp': '2026-xx-xxTxx:xx:xx.xxxZ',

      -- Metadados internos
      _run_id,
      timestamp(_extracted_at) as _extracted_at,
      date(data_particao) as data_particao,

    from unnested
  )

select *
from sisreg
