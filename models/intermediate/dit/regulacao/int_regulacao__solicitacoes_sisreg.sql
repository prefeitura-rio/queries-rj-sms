{{
  config(
    schema="intermediario_regulacao",
    alias="solicitacoes_sisreg",
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
      paciente_cns,

      sigla_situacao,  -- P, A, D, N, C, R
      tipo_regulacao_codigo,  -- grande maioria "R"; um "F"
      tipo_fila_codigo,       -- grande maioria "1"; um "2"
      executante_crm,
      -- operador_videofonista_nome,  -- ?

      ----------------
      -- Solicitação
      struct(
        codigo_solicitacao as id,
        datahora_solicitacao,
        datahora_atualizacao,
        status_solicitacao as status,  -- ex. "SOLICITAÇÃO / DEVOLVIDA / REGULADOR"
        solicitacao_visualizada_regulador as visualizada_regulador,
        tipo_vaga_solicitada as tipo_vaga,
        data_desejada,
        unidade_desejada_id_cnes,
        unidade_desejada_nome
      ) as solicitacao,

      ----------------
      -- Cancelamento
      struct(
        datahora_cancelamento,
        -- perfil_cancelamento_codigo,
        perfil_cancelamento_nome as perfil,  -- solicitante, regulador/autorizador, ...
        operador_cancelamento_nome as operador_nome
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
        procedimento_descricao as descricao,
        procedimento_sigtap_id as sigtap_id,
        procedimento_sigtap_descricao as sigtap_descricao
      ) as procedimento,

      ----------------
      -- Solicitante
      struct(
        -- uf_solicitante_codigo,
        uf_solicitante_sigla as uf_sigla,
        -- central_solicitante_id_cnes,
        -- central_solicitante_codigo,
        -- central_solicitante_nome_cnes,
        central_solicitante_nome as central_nome,

        unidade_solicitante_id_cnes as unidade_id_cnes,
        unidade_solicitante_nome as unidade_nome,

        profissional_solicitante_cpf as profissional_cpf,
        {{ proper_br("medico_solicitante_nome") }} as profissional_nome,

        operador_solicitante_nome as operador_nome
      ) as solicitante,

      ----------------
      -- Regulador
      struct(
        uf_regulador_codigo,  -- sempre '33'
        uf_regulador_sigla,  -- sempre 'RJ'
        central_reguladora_codigo,  -- sempre '330455', é filtro na extração
        central_reguladora_nome  -- sempre 'RIO DE JANEIRO'
      ) as regulador,

      ----------------
      -- Laudo
      struct(
        laudo_operador_id_cnes as operador_id_cnes,
        laudo_operador_unidade_nome as operador_unidade,

        cid_id,
        cid_descricao,

        laudo_perfil_tipo as perfil_tipo,
        laudo_descricao_tipo as descricao_tipo,
        laudo_situacao as situacao,
        laudo_observacao as observacao,
        laudo_datahora_observacao as datahora_observacao
      ) as laudo,

      _run_id,
      _extracted_at,
      data_particao

    from {{ ref("raw_sisreg_api_v2__solicitacao_ambulatorial") }}
  )

select *
from source
