{{
    config(
        alias="medicamentos_prescritos"
    )
}}
with

cadastros as (
  select
    cpf as paciente_id
  from {{ ref('raw_prontuario_vitacare_historico__cadastro') }}
  where
    id_cnes = '2280787'
    and cpf is not NULL
    and ine_equipe is not NULL
)

SELECT 
  sha256(a.patient_cpf) as paciente_id,
  medicamento_nome,
  posologia,
  quantidade,
  uso_continuado 
FROM {{ ref('raw_prontuario_vitacare_historico__prescricao') }} p
  INNER JOIN {{ ref('raw_prontuario_vitacare_historico__acto') }} a using (id_prontuario_global)
WHERE 
  a.patient_cpf is not null AND
  a.patient_cpf in (select paciente_id from cadastros)