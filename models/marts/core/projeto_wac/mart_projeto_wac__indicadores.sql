{{
    config(
        alias="indicadores"
    )
}}

with

cadastros as (
  select
    cpf as paciente_id,
  from {{ ref('raw_prontuario_vitacare_historico__cadastro') }}
  where
    id_cnes = '2280787'
    and cpf is not NULL
    and ine_equipe is not NULL
),

valores as (
  select a.patient_cpf as paciente_id, indicadores_nome, valor, a.datahora_fim_atendimento as registrado_em
  from {{ ref('raw_prontuario_vitacare_historico__indicador') }}
    INNER JOIN {{ ref('raw_prontuario_vitacare_historico__acto') }} a using (id_prontuario_global)
  where indicadores_nome in ('IMC','Pressão arterial diastólica','Pressão Arterial Sistólica')
)

SELECT 
    sha256(paciente_id) as paciente_id, 
    indicadores_nome as nome,
    valor,
    registrado_em
FROM valores
WHERE 
    paciente_id is not null and
    paciente_id in (select paciente_id from cadastros)