{{
  config(
    alias="regulacao",
    schema="app_historico_clinico_treinamento",
    materialized="table",
    partition_by={
      "field": "cpf_particao",
      "data_type": "int64",
      "range": {"start": 0, "end": 100000000000, "interval": 34722222},
    },
  )
}}

select
  "123456789" as solicitacao_id,
  datetime("2026-04-01T20:01:23.000000") as solicitacao_datahora,
  datetime("2026-04-02T07:02:35.000000") as atualizacao_datahora,
  "AGENDAMENTO" as detalhe_tipo,
  "CONFIRMADO" as detalhe_status,
  "EXECUTANTE" as detalhe_responsavel,
  "verde" as classificacao_risco,
  cast(null as date) as data_desejada,
  cast(null as string) as unidade_desejada,
  "Consulta em Dermatologia" as procedimento_descricao,
  "CMS Parque União" as unidade_solicitante,
  "Maria da Graça M" as profissional_solicitante,

  [
    struct(
      "L989" as cid_id,
      "AFECCOES DA PELE E DO TECIDO SUBCUTANEO, NAO ESPECIFICADOS" as cid_descricao,
      "Solicitante" as perfil_tipo,
      "Observação" as descricao_tipo,
      "PENDENTE" as situacao,
      trim("""
Paciente possui afecções não especificadas de pele e tecido subcutâneo após sofrer queda em playground (W096).
Requisito opinião profissional de dermatologia.
      """) as observacao,
      datetime("2026-04-01T20:01:23.000000") as datahora_observacao,
      "CMS Parque União" as operador_unidade
    ),
    struct(
      "L989",
      "AFECCOES DA PELE E DO TECIDO SUBCUTANEO, NAO ESPECIFICADOS",
      "Regulador" as perfil_tipo,
      "Justificativa" as descricao_tipo,
      "DEVOLVIDO" as situacao,
      "Prezado, favor informar peso" as observacao,
      datetime("2026-04-02T07:00:01.000000"),
      "Complexo Regulador da Cidade do Rio de Janeiro"
    ),
    struct(
      "L989",
      "AFECCOES DA PELE E DO TECIDO SUBCUTANEO, NAO ESPECIFICADOS",
      "Solicitante" as perfil_tipo,
      "Observação" as descricao_tipo,
      "REENVIADO" as situacao,
      "80 kg" as observacao,
      datetime("2026-04-02T07:02:34.000000"),
      "CMS Parque União"
    )
  ] as laudo,

  [
    struct(
      "9876543210" as id,
      datetime("2026-04-03T08:30:00") as datahora,
      datetime("2026-04-02T07:33:44.000000") as aprovacao_datahora,
      cast(null as date) as confirmacao_data,
      "sim" as flag_paciente_avisado,
      "nao" as flag_executada,
      "nao" as flag_falta_registrada
    )
  ] as marcacao,

  [
    struct(
      "Sasha M Szafir" as profissional_nome,
      "Centro Medico Cadeg" as unidade_nome,
      "Rio de Janeiro" as unidade_municipio,
      "Benfica" as unidade_bairro
    )
  ] as execucao,

  "sisreg" as fonte,
  42298037299 as cpf_particao


union all


select
  "987654321" as solicitacao_id,
  datetime("2025-08-01T10:20:30.400000") as solicitacao_datahora,
  datetime("2025-08-01T20:10:05.025000") as atualizacao_datahora,
  "SOLICITAÇÃO" as detalhe_tipo,
  "CANCELADO" as detalhe_status,
  "SOLICITANTE" as detalhe_responsavel,
  "vermelho" as classificacao_risco,
  cast(null as date) as data_desejada,
  cast(null as string) as unidade_desejada,
  "Colonoscopia" as procedimento_descricao,
  "CMS Parque União" as unidade_solicitante,
  "Maria da Graça M" as profissional_solicitante,

  [
    struct(
      "X359" as cid_id,
      "VITIMA DE ERUPCAO VULCANICA - LOCAL NAO ESPECIFICADO" as cid_descricao,
      "Solicitante" as perfil_tipo,
      "Observação" as descricao_tipo,
      "PENDENTE" as situacao,
      trim("""
Residente do Rio de Janeiro, paciente se recusa a informar onde encontrou um vulcão em erupção na cidade.
Requisito colonoscopia como punição por não me dizer onde fica o vulcão, eu queria muito ver.
      """) as observacao,
      datetime("2025-08-01T10:20:30.400000") as datahora_observacao,
      "CMS Parque União" as operador_unidade
    ),
    struct(
      "X359",
      "VITIMA DE ERUPCAO VULCANICA - LOCAL NAO ESPECIFICADO",
      "Solicitante" as perfil_tipo,
      "Justificativa" as descricao_tipo,
      "CANCELADO" as situacao,
      "Após ameaças, usuário relatou onde fica o vulcão." as observacao,
      datetime("2025-08-01T15:43:21.000000"),
      "CMS Parque União"
    )
  ] as laudo,

  [] as marcacao,

  [] as execucao,

  "sisreg" as fonte,
  42298037299 as cpf_particao
