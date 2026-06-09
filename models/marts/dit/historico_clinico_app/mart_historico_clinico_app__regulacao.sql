{{
  config(
    schema="app_historico_clinico",
    alias="regulacao",
    materialized="table",
    partition_by={
      "field": "cpf_particao",
      "data_type": "int64",
      "range": {"start": 0, "end": 100000000000, "interval": 34722222},
    },
  )
}}

with
  source as (
    select
      s.solicitacao.id as solicitacao_id,
      s.solicitacao.solicitacao_datahora,
      s.solicitacao.atualizacao_datahora,
      s.cancelamento.datahora as cancelamento_datahora,

      s.solicitacao.detalhe_tipo,
      s.solicitacao.detalhe_status,
      s.solicitacao.detalhe_responsavel,

      s.solicitacao.data_desejada,
      {{
        estabelecimento_remove_apendices(
          proper_estabelecimento("s.solicitacao.unidade_desejada_nome")
        )
      }} as unidade_desejada,

      s.procedimento.sigtap_id,
      s.procedimento.sigtap_descricao,

      {{
        estabelecimento_remove_apendices(
          proper_estabelecimento("s.solicitante.unidade_nome")
        )
      }} as unidade_solicitante,
      s.solicitante.profissional_nome as profissional_solicitante,

      s.laudo.cid_id as laudo_cid_id,
      s.laudo.cid_descricao as laudo_cid_descricao,
      s.laudo.descricao_tipo as laudo_descricao_tipo,
      s.laudo.situacao as laudo_situacao,
      s.laudo.observacao as laudo_observacao,
      s.laudo.datahora_observacao as laudo_datahora,
      {{
        estabelecimento_remove_apendices(
          proper_estabelecimento("s.laudo.operador_unidade")
        )
      }} as laudo_operador_unidade,

      s.marcacao.id as marcacao_id,
      s.marcacao.datahora as marcacao_datahora,
      s.marcacao.aprovacao_datahora,
      s.marcacao.confirmacao_data,
      s.marcacao.flag_paciente_avisado,
      s.marcacao.flag_executada,
      s.marcacao.flag_falta_registrada,

      s.execucao.profissional_nome as executante_profissional,
      {{
        estabelecimento_remove_apendices(
          proper_estabelecimento("s.execucao.unidade_nome")
        )
      }} as executante_unidade,
      {{ proper_br("s.execucao.unidade_municipio") }} as executante_municipio,
      {{ proper_br("s.execucao.unidade_bairro") }} as executante_bairro,

      s.fonte,
      s.cpf_particao
    from {{ ref("mart_regulacao__solicitacao") }} as s
  )

select *
from source
