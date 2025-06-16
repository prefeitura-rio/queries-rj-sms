{{
    config(
        enabled=true,
        alias="atendimentos_csat",
        materialized="table",
        partition_by={
            "field": "dia_atendimento",
            "data_type": "date",
            "granularity": "day",
        },
    )
}}

WITH

unidades as (
  select 
    id_cnes,
    nome_limpo as nome_unidade,
    array_agg({{ padronize_telefone('telefone') }}) as telefone_unidade
  from {{ ref('dim_estabelecimento') }}, unnest(telefone) as telefone
  group by 1,2
),
atendimentos as (
  SELECT
    cpf,
    struct(
      nome_profissional as nome,
      cbo_descricao_profissional as cargo
    ) as profissional,
    tipo as tipo_atendimento,
    datahora_fim as horario_atendimento
  FROM {{ ref('raw_prontuario_vitacare__atendimento') }} as atend
    inner join unidades on unidades.id_cnes = atend.cnes_unidade
  WHERE tipo not in (
    'Gestão de Arquivo de Enfermagem',
    'Gestão de Ficheiro de Enfermagem',
    'Ficha da Aula'
  )
)
SELECT
  struct(
    paciente.cpf,
    paciente.dados.nome
  ) as paciente,
  atendimentos.profissional,
  atendimentos.tipo_atendimento,
  safe_cast(atendimentos.horario_atendimento as timestamp) as horario_atendimento,
  safe_cast(atendimentos.horario_atendimento as date) as dia_atendimento
FROM atendimentos
  inner join {{ ref('mart_historico_clinico__paciente') }} paciente on paciente.cpf = atendimentos.cpf