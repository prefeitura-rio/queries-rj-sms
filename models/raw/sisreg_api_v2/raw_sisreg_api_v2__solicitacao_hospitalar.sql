{{
  config(
    schema = "brutos_sisreg_api_v2",
    alias  = "solicitacao_hospitalar",
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
    from {{ source("brutos_sisreg_api_v2_staging", "solicitacao_hospitalar_rj") }}
    qualify row_number() over (
      partition by codigo_solicitacao
      order by _extracted_at desc nulls last
    ) = 1
  ),

  sisreg as (
    select
      -- Identificação básica da solicitação
      cast({{ process_null("codigo_solicitacao") }} as string) as solicitacao_id,
      cast({{ process_null("data_solicitacao") }} as string)  as solicitacao_data,
      cast({{ process_null("hora_solicitacao") }} as string) as solicitacao_hora,
      cast({{ process_null("data_atualizacao") }} as timestamp)  as atualizacao_datahora,
      cast({{ process_null("data_atualizacao_solicitacao") }} as timestamp) as solicitacao_atualizacao_datahora,
      cast({{ process_null("data_atualizacao_marcacao") }} as timestamp) as marcacao_atualizacao_datahora,


      -- Status e classificação
      cast({{ process_null("status") }} as string) as solicitacao_status,

      cast({{ process_null("codigo_classificacao_risco") }} as string) as classificacao_risco_codigo,
      case trim(codigo_classificacao_risco)
          when "1" then "vermelho"  -- até 30 dias
          when "2" then "amarelo"   -- até 90 dias
          when "3" then "verde"     -- até 180 dias
          when "4" then "azul"      -- >180 dias
          else cast(null as string)
      end as classificacao_risco,
      cast({{ process_null("carater") }} as string) as carater,
      cast({{ process_null("codigo_natureza_lesao") }} as string) as lesao_codigo,
      cast({{ process_null("descricao_natureza_lesao") }} as string) as lesao_descricao,


      -- Informações da solicitação
      upper({{ process_null("codigo_cid") }}) as cid_id,
      cast({{ process_null("descricao_cid") }} as string) as cid_descricao,
      cast({{ process_null("sintomas") }} as string) as sintomas,
      cast({{ process_null("exames") }} as string) as exames,
      cast({{ process_null("justificativa") }} as string) as justificativa,
      cast({{ process_null("justificativa_impedimento") }} as string) as justificativa_impedimento,


      -- Dados do procedimento
      {{ process_null("codigo_procedimento") }} as procedimento_sigtap_id,
      {{ process_null("descricao_procedimento") }} as procedimento_sigtap_descricao,


      -- Dados do solicitante
      cast({{ process_null("codigo_uf_solicitante") }} as string) as uf_solicitante_codigo,
      cast({{ process_null("sigla_uf_solicitante") }} as string) as uf_solicitante_sigla,

      cast({{ process_null("codigo_central_solicitante") }} as string) as central_solicitante_codigo_ibge,
      cast({{ process_null("nome_central_solicitante") }} as string) as central_solicitante_nome,

      lpad({{ process_null("codigo_unidade_solicitante") }}, 7, "0") as unidade_solicitante_id_cnes,
      trim({{ process_null("nome_unidade_solicitante") }}) as unidade_solicitante_nome,

      lpad({{ process_null("cpf_medico_solicitante") }}, 11, "0") as medico_solicitante_cpf,
      cast({{ process_null("nome_medico_solicitante") }} as string) as medico_solicitante_nome,


      -- Dados dos operadores
      cast({{ process_null("nome_operador_solicitante") }} as string) as operador_solicitante_nome,


      -- Dados do regulador
      cast({{ process_null("codigo_uf_regulador") }} as string) as uf_regulador_codigo_ibge,
      cast({{ process_null("sigla_uf_regulador") }} as string) as uf_regulador_sigla,

      cast({{ process_null("codigo_central_reguladora") }} as string) as central_reguladora_codigo_ibge,
      cast({{ process_null("nome_central_reguladora") }} as string) as central_reguladora_nome,


      -- Cancelamento
      --- (nada)


      -- Dados do executante
      cast({{ process_null("cpf_profissional_executante") }} as string) as profissional_executante_cpf,
      cast({{ process_null("nome_unidade_executante") }} as string) as unidade_executante_nome,
      cast({{ process_null("codigo_unidade_executante") }} as string) as unidade_executante_codigo,
      cast({{ process_null("municipio_unidade_executante") }} as string) as unidade_executante_municipio,
      cast({{ process_null("bairro_unidade_executante") }} as string) as unidade_executante_bairro,
      cast({{ process_null("cep_unidade_executante") }} as string) as unidade_executante_cep,
      cast({{ process_null("logradouro_unidade_executante") }} as string) as unidade_executante_logradouro,
      cast({{ process_null("numero_unidade_executante") }} as string) as unidade_executante_numero,
      cast({{ process_null("complemento_unidade_executante") }} as string) as unidade_executante_complemento,
      cast({{ process_null("telefone_unidade_executante") }} as string) as unidade_executante_telefone,


      -- Preferências da solicitação
      date(
        cast({{ process_null("data_desejada") }} as timestamp)
      ) as data_desejada,
      lpad({{ process_null("codigo_unidade_desejada") }}, 7, "0") as unidade_desejada_id_cnes,
      cast({{ process_null("nome_unidade_desejada") }} as string) as unidade_desejada_nome,


      -- Dados do paciente
      lpad({{ process_null("cpf_usuario") }}, 11, "0") as paciente_cpf,
      lpad({{ process_null("cns_usuario") }}, 15, "0") as paciente_cns,
      cast({{ process_null("no_usuario") }} as string) as paciente_nome,
      lower({{ process_null("sexo_usuario") }}) as paciente_sexo,
      cast({{ process_null("no_mae_usuario") }} as string) as paciente_nome_mae,
      cast({{ process_null("telefone") }} as string) as paciente_telefones,
      -- -- Nascimento
      date({{ process_null("dt_nascimento_usuario") }}) as paciente_nascimento_data,
      cast({{ process_null("uf_municipio_nascimento") }} as string) as paciente_nascimento_uf,
      cast({{ process_null("nome_municipio_nascimento") }} as string) as paciente_nascimento_municipio,
      -- -- Residência
      lpad({{ process_null("cep_paciente_residencia") }}, 8, "0") as paciente_residencia_cep,
      cast({{ process_null("uf_paciente_residencia") }} as string) as paciente_residencia_uf,
      cast({{ process_null("municipio_paciente_residencia") }} as string) as paciente_residencia_municipio,
      cast({{ process_null("bairro_paciente_residencia") }} as string) as paciente_residencia_bairro,
      cast({{ process_null("tipo_logradouro_paciente_residencia") }} as string) as paciente_residencia_logradouro,
      cast({{ process_null("endereco_paciente_residencia") }} as string) as paciente_residencia_endereco,
      cast({{ process_null("complemento_paciente_residencia") }} as string) as paciente_residencia_complemento,
      cast({{ process_null("numero_paciente_residencia") }} as string) as paciente_residencia_numero,


      -- Responsável
      cast({{ process_null("nome_responsavel") }} as string) as responsavel_nome,
      cast({{ process_null("telefone_responsavel") }} as string) as responsavel_telefone,

      -- Alta
      cast({{ process_null("data_previsao_alta") }} as string) as alta_data_prevista,
      cast({{ process_null("data_alta") }} as string) as alta_data,
      cast({{ process_null("hora_alta") }} as string) as alta_hora,
      cast({{ process_null("operador_alta") }} as string) as alta_operador,

      -- Misc
      cast({{ process_null("numero_aih") }} as string) as aih_numero,
      cast({{ process_null("numero_digito_aih") }} as string) as aih_digito,
      cast({{ process_null("valor_proc_aih") }} as string) as aih_valor_procedimento,

      cast({{ process_null("nome_clinica") }} as string) as clinica_nome,
      cast({{ process_null("nome_clinica_complementar") }} as string) as clinica_nome_complementar,

      cast({{ process_null("codigo_unidade_referencia") }} as string) as unidade_referencia_codigo,
      cast({{ process_null("nome_unidade_referencia") }} as string) as unidade_referencia_nome,

      cast({{ process_null("hora_autorizacao") }} as string) as autorizacao_hora,
      cast({{ process_null("nome_operador_autorizacao") }} as string) as autorizacao_operador_nome,

      cast({{ process_null("data_internacao") }} as string) as internacao_data,
      cast({{ process_null("operador_internacao") }} as string) as internacao_operador,

      cast({{ process_null("nome_leito") }} as string) as leito_nome,
      cast({{ process_null("nome_leito_complementar") }} as string) as leito_complementar_nome,

      cast({{ process_null("data_reserva") }} as string) as reserva_data,

      cast({{ process_null("version") }} as string) as versao_sisreg,
      cast({{ process_null("type") }} as string) as tipo_interno,
      "solicitacao-hospitalar" as tipo_externo,

      -- Campos deixados de fora:
      --   'carga_epoch': '1779xxxxxx',
      --   'timestamp': '2026-xx-xxTxx:xx:xx.xxxZ',

      -- Metadados internos
      _run_id,
      timestamp(_extracted_at) as _extracted_at,
      date(data_particao) as data_particao

    from dedup_source
  )

select *
from sisreg
