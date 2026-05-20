{{
    config(
        alias="visitas"
    )
}}

select
  {{ anonimize('profissional_cpf', "'hackathon_anthropic'") }} as profissional_id,
  {{ anonimize('profissional_equipe_cod_ine', "'hackathon_anthropic'") }} as equipe_id,
  {{ anonimize('patient_cpf', "'hackathon_anthropic'") }} as paciente_id,
  datahora_fim_atendimento as visitado_em
from {{ ref('raw_prontuario_vitacare_historico__acto') }}
where
  tipo_consulta = 'Visita Domiciliar'
  and unidade_ap = '22'
  and datahora_fim_atendimento between '2025-01-01' and '2025-12-31'
  and patient_cpf is not null
  and profissional_equipe_cod_ine is not null
