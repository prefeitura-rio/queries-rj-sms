{{
    config(
        alias="medicamentos_prescritos"
    )
}}

SELECT 
  sha256(a.patient_cpf) as paciente_id,
  medicamento_nome,
  posologia,
  quantidade,
  uso_continuado 
FROM `rj-sms.brutos_prontuario_vitacare_historico.prescricao` p
  INNER JOIN `rj-sms.brutos_prontuario_vitacare_historico.acto` a using (id_prontuario_global)
WHERE 
  a.id_cnes = '2280787' and 
  a.patient_cpf is not null