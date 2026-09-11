{{
    config(
        alias="visitas"
    )
}}

with

constantes as (
  select
    date('2025-01-01') as data_minima,
    date('2026-12-31') as data_maxima
),

elegiveis as (
  select
    paciente_cpf
  from {{ ref('mart_visitare_app__elegiveis') }}
),

-- Buscar todas as visitas domiciliares no período
visitas_brutas as (
  select
    a.id_prontuario_global as visita_id,
    a.cpf_profissional as profissional_cpf,
    a.cpf as paciente_cpf,
    a.datahora_fim as datahora_visita
  from {{ ref('raw_prontuario_vitacare__atendimento') }} a
    inner join elegiveis e on a.cpf = e.paciente_cpf
    cross join constantes
  where
    a.tipo_consulta = 'Visita Domiciliar'
    and a.cbo_descricao_profissional in ('Agente comunitário de saúde', 'Técnico em Agente Comunitário de Saúde')
    and a.cpf is not null
    and a.cpf_profissional is not null
    and date(a.datahora_fim) between (select data_minima from constantes) and (select data_maxima from constantes)
),

-- Remover duplicatas por profissional + paciente + data
-- Em caso de duplicata, mantém apenas uma ocorrência
visitas_deduplicadas as (
  select
    visita_id,
    profissional_cpf,
    paciente_cpf,
    datahora_visita
  from visitas_brutas
  qualify row_number() over (
    partition by visita_id
    order by datahora_visita
  ) = 1
)

select
  visita_id,
  profissional_cpf,
  paciente_cpf,
  datahora_visita as registrados_em
from visitas_deduplicadas
