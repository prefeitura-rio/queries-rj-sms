{{
    config(
        enabled=true,
        alias="agendamentos_sisreg",
        materialized="table",
        partition_by={
            "field": "dia_marcacao",
            "data_type": "date",
            "granularity": "day",
        },
    )
}}

SELECT
  struct(
    marcacoes.paciente_cpf as cpf,
    marcacoes.paciente_nome as nome
  ) as paciente,

  procedimento_sigtap as nome_procedimento_geral,
  procedimento_interno as nome_procedimento_especifico,
  safe_cast(data_marcacao as date) as dia_marcacao,

  struct(
    unidade_executante_nome as nome,
    unidade_executante_id as cnes,
    unidade_executante_logradouro as logradouro,
    unidade_executante_complemento as complemento,
    unidade_executante_numero as numero,
    unidade_executante_bairro as bairro,
    unidade_executante_municipio as municipio,
    {{ padronize_telefone('unidade_executante_telefone') }} as telefone
  ) as unidade
FROM {{ ref("raw_sisreg_api__marcacoes") }} marcacoes
WHERE 
  solicitacao_status in (
      "SOLICITAÇÃO / AGENDADA / COORDENADOR",
      "SOLICITAÇÃO / AGENDADA / SOLICITANTE"
  ) 
  and safe_cast(data_marcacao as date) >= current_date()