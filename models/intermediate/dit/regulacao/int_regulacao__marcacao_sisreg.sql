{{
  config(
    schema="intermediario_regulacao",
    alias="marcacao_sisreg",
    materialized="table",
    partition_by={
      "field": "data_particao",
      "data_type": "date",
      "granularity": "month"
    },
  )
}}

with
  source as (
    select
      paciente_cpf,
      paciente_cns,

      ----------------
      -- Solicitação
      struct(
        solicitacao_id as id,
        datetime(
          solicitacao_datahora,
          "America/Sao_Paulo"
        ) as solicitacao_datahora,
        datetime(atualizacao_datahora, "America/Sao_Paulo") as atualizacao_datahora,

        -- ex. "SOLICITAÇÃO / DEVOLVIDA / REGULADOR"
        trim(split(solicitacao_status, "/")[safe_offset(0)]) as detalhe_tipo,
        trim(split(solicitacao_status, "/")[safe_offset(1)]) as detalhe_status,
        trim(split(solicitacao_status, "/")[safe_offset(2)]) as detalhe_responsavel,

        solicitacao_visualizada_regulador as visualizada_regulador,
        tipo_vaga_solicitada as tipo_vaga,
        classificacao_risco,
        data_desejada,
        unidade_desejada_id_cnes,
        {{ add_accents_estabelecimento("unidade_desejada_nome") }} as unidade_desejada_nome
      ) as solicitacao,

      ----------------
      -- Cancelamento
      struct(
        datetime(cancelamento_datahora, "America/Sao_Paulo") as datahora,
        cancelamento_justificativa as justificativa,
        {{ proper_br("perfil_cancelamento_nome") }} as perfil
      ) as cancelamento,

      ----------------
      -- Procedimento
      struct(
        procedimento_grupo_codigo as grupo_codigo, -- Código de grupo sempre termina em '000'
        regexp_replace(
          procedimento_grupo_nome,
          r"^GRUPO\s*-\s*",
          ""
        ) as grupo_nome, -- Nome sempre começa com "GRUPO - "
        procedimento_id as id,
        trim(procedimento_descricao) as descricao,
        procedimento_sigtap_id as sigtap_id,
        trim(procedimento_sigtap_descricao) as sigtap_descricao
      ) as procedimento,

      ----------------
      -- Solicitante
      struct(
        -- uf_solicitante_codigo,
        uf_solicitante_sigla as uf_sigla,
        -- central_solicitante_id_cnes,
        -- central_solicitante_codigo_ibge,
        -- central_solicitante_nome_cnes,
        central_solicitante_nome as central_nome,

        unidade_solicitante_id_cnes as unidade_id_cnes,
        {{ add_accents_estabelecimento("unidade_solicitante_nome") }} as unidade_nome,

        profissional_solicitante_cpf as profissional_cpf,
        {{ proper_br("medico_solicitante_nome") }} as profissional_nome

        -- operador_solicitante_nome
      ) as solicitante,

      ----------------
      -- Regulador
      struct(
        uf_regulador_codigo_ibge,  -- sempre '33'
        uf_regulador_sigla,  -- sempre 'RJ'
        central_reguladora_codigo_ibge as central_reguladora_codigo,  -- sempre '330455', é filtro na extração
        central_reguladora_nome  -- sempre 'RIO DE JANEIRO'
      ) as regulador,

      ----------------
      -- Laudo
      struct(
        laudo_operador_id_cnes as operador_id_cnes,
        {{ add_accents_estabelecimento("laudo_operador_unidade_nome") }} as operador_unidade,

        cid_id,
        cid_descricao,

        laudo_perfil_tipo as perfil_tipo,
        laudo_descricao_tipo as descricao_tipo,
        laudo_situacao as situacao,
        laudo_observacao as observacao,
        datetime(
          laudo_datahora_observacao,
          "America/Sao_Paulo"
        ) as datahora_observacao
      ) as laudo,

      ----------------
      -- Marcação
      struct(
        marcacao_id as id,
        datetime(
          marcacao_data,
          "America/Sao_Paulo"
        ) as datahora,
        datetime(
          -- TODO: acho que o UTC daqui é fake, e a hora já está no fuso certo
          -- o que traz o questionamento: os outros também já estão?
          aprovacao_datahora,
          "America/Sao_Paulo"
        ) as aprovacao_datahora,
        date(confirmacao_data) as confirmacao_data,
        flag_paciente_avisado,
        marcacao_executada as flag_executada,
        flag_falta_registrada
      ) as marcacao,

      ----------------
      -- Execução
      struct(
        profissional_executante_crm as profissional_crm,
        profissional_executante_cpf as profissional_cpf,
        {{ proper_br("profissional_executante_nome") }} as profissional_nome,
        unidade_executante_id_cnes as unidade_id_cnes,
        unidade_executante_nome as unidade_nome,
        unidade_executante_telefone as unidade_telefone,
        unidade_executante_cep as unidade_cep,
        unidade_executante_municipio as unidade_municipio,
        unidade_executante_bairro as unidade_bairro,
        unidade_executante_logradouro as unidade_logradouro,
        unidade_executante_numero as unidade_numero,
        unidade_executante_complemento as unidade_complemento
      ) as execucao,

      tipo_externo as tipo,
      _run_id,
      datetime(
        _extracted_at,
        "America/Sao_Paulo"
      ) as _extracted_at,
      data_particao

    from {{ ref("raw_sisreg_api_v2__marcacao_ambulatorial") }}
  )

select *
from source
