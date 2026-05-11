{{
    config(
        alias="visitas"
    )
}}

select
  SHA256(profissional_cpf) as profissional_id,
  SHA256(profissional_equipe_cod_ine) as equipe_id,
  SHA256(patient_cpf) as paciente_id,
  datahora_fim_atendimento as visitado_em
from {{ ref('raw_prontuario_vitacare_historico__acto') }}
where
  tipo_consulta = 'Visita Domiciliar'
  and unidade_ap = '22'
  and datahora_fim_atendimento between '2025-01-01' and '2025-12-31'
  and patient_cpf is not null
  and profissional_equipe_cod_ine is not null
