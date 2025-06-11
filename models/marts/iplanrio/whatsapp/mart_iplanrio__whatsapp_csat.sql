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

telefones as (
  select 
    cpf,
    array_agg(
      struct(
        numero_telefone.ddd,
        numero_telefone.valor
      )
    ) as telefones
  from {{ ref('mart_historico_clinico__paciente') }},
    unnest(contato.telefone) as numero_telefone
  where 
    starts_with(numero_telefone.valor, '9') and 
    length(numero_telefone.valor) = 9
  group by 1
),
unidades as (
  select 
    id_cnes,
    nome as nome_unidade,
    {{ padronize_telefone('telefone') }} as telefone_unidade
  from {{ ref('dim_estabelecimento') }}
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
  FROM {{ ref('raw_prontuario_vitacare__atendimento') }}
    inner join unidades on unidades.id_cnes = atendimentos.cnes_unidade
  WHERE tipo not in (
    'Gestão de Arquivo de Enfermagem',
    'Gestão de Ficheiro de Enfermagem',
    'Ficha da Aula'
  )
)
SELECT
  struct(
    paciente.cpf,
    paciente.dados.nome,
    telefones.telefones[offset(0)].ddd as telefone_ddd,
    telefones.telefones[offset(0)].valor as telefone_numero
  ) as paciente,
  atendimentos.profissional,
  atendimentos.tipo_atendimento,
  safe_cast(atendimentos.horario_atendimento as timestamp) as horario_atendimento,
  safe_cast(atendimentos.horario_atendimento as date) as dia_atendimento
FROM atendimentos
  inner join {{ ref('mart_historico_clinico__paciente') }} paciente on paciente.cpf = atendimentos.cpf
  inner join telefones on telefones.cpf = atendimentos.cpf