{{
    config(
        schema="brutos_prontuario_carioca_saude_mental_prescricao",
        alias="atendimentos",
        materialized="table",
        tags=["raw", "pcsm", "atendimentos"],
        description="Atendimentos registrados em hospitais e unidades de saúde municipais do Rio de Janeiro."
    )
}}

with source as (
    select
        safe_cast(id_atendimento as int64) as id_atendimento,                         -- Identificador único do atendimento
        safe_cast(id_paciente as int64) as id_paciente,                               -- Identificador único do paciente
        safe_cast(crm as int64) as conselho_regional_medicina,                        -- CRM do médico responsável pelo atendimento
        safe_cast(leito as string) as numero_leito,                                   -- Número ou identificação do leito do paciente
        safe_cast( {{process_null('clinica')}} as string) as nome_clinica,                                 -- Clínica onde o atendimento está ocorrendo
        safe_cast(recomendacoes as string) as orientacao_medica,                      -- Recomendações médicas ou observações gerais
        safe_cast(data_hora as datetime) as data_hora_registro,                       -- Data e hora exatas do atendimento
        safe_cast(data_internacao as date) as data_internacao,                        -- Data de internação do paciente
        safe_cast(data_atendimento as date) as data_atendimento,                      -- Data do atendimento
        safe_cast(ativo as int64) as registro_ativo,                                  -- Indica se o registro de atendimento está ativo (1) ou inativo (0)
        case trim(safe_cast(ativo as string))
            when '1' then 'Ativo'
            when '0' then 'Inativo'
            when '' then 'Não informado'
            when null then 'Não informado'
            else 'Não classificado'
        end as descricao_status_atendimento,                                         -- Descrição do status do atendimento
        safe_cast( {{process_null('cod_clin')}} as string) as codigo_clinica,                              -- Código da clínica
        safe_cast( {{process_null('enfermaria')}} as string) as nome_enfermaria,                           -- Identificação da enfermaria
        safe_cast( {{process_null('parenteral')}} as string) as medicamento_parenteral,                    -- Informações sobre nutrição parenteral
        safe_cast( {{process_null('peso')}} as string) as peso_paciente,                                   -- Peso do paciente
        safe_cast(status_parenteral as string) as status_parenteral,                  -- Status da nutrição parenteral
        case trim(safe_cast(status_parenteral as string))
            when '1' then 'Ativo'
            when '0' then 'Inativo'
            when '' then 'Não informado'
            when null then 'Não informado'
            else 'Não classificado'
        end as descricao_status_parenteral,                                         -- Descrição do status da nutrição parenteral
        safe_cast( {{process_null('paren_neo')}} as string) as nutricao_parenteral,                        -- Informações sobre nutrição parenteral neonatal
        safe_cast( {{process_null('paren_venosa')}} as string) as medicamento_venoso_parenteral,           -- Informações sobre nutrição parenteral via venosa
        safe_cast(status_paren_neo as string) as status_nutricao_parenteral_neonatal,          -- Status da nutrição parenteral neonatal
        case trim(safe_cast(status_paren_neo as string))
            when '1' then 'Ativo'
            when '0' then 'Inativo'
            when '' then 'Não informado'
            when null then 'Não informado'
            else 'Não classificado'
        end as descricao_status_nutricao_parenteral_neonatal,                                -- Descrição do status da nutrição parenteral neonatal
        safe_cast(status_paren_venosa as string) as status_nutricao_parenteral_venosa,         -- Status da nutrição parenteral venosa 
        case trim(safe_cast(status_paren_venosa as string))
            when '1' then 'Ativo'
            when '0' then 'Inativo'
            when '' then 'Não informado'
            when null then 'Não informado'
            else 'Não classificado'
        end as descricao_status_nutricao_parenteral_venosa,                               -- Descrição do status da nutrição parenteral venosa
        safe_cast(precisa_fono as int64) as precisa_fono,                             -- Precisa de acompanhamento fonoaudiológico
        case trim(safe_cast(precisa_fono as string))
            when '1' then 'Sim'
            when '0' then 'Não'
            when '' then 'Não informado'
            when null then 'Não informado'
            else 'Não classificado'
        end as descricao_precisa_fono,                                               -- Descrição se precisa de acompanhamento fonoaudiológico
        safe_cast(precisa_fisio as int64) as precisa_fisio,                           -- Precisa de acompanhamento fisioterapêutico
        case trim(safe_cast(precisa_fisio as string))
            when '1' then 'Sim'
            when '0' then 'Não'
            when '' then 'Não informado'
            when null then 'Não informado'
            else 'Não classificado'
        end as descricao_precisa_fisio,                                             -- Descrição se precisa de acompanhamento fisioterapêutico 
        safe_cast( {{process_null('obs_fisio')}} as string) as observacao_fisio,                           -- Observações da fisioterapia
        safe_cast( {{process_null('obs_fono')}} as string) as observacao_fono,                             -- Observações da fonoaudiologia
        safe_cast(fono_ok as int64) as fono_concluido,                                -- Acompanhamento fonoaudiológico realizado/concluído
        case trim(safe_cast(fono_ok as string))
            when '1' then 'Realizado/Concluído'
            when '0' then 'Pendente'
            when '' then 'Não informado'
            when null then 'Não informado'
            else 'Não classificado'
        end as descricao_fono_concluido,                                           -- Descrição se o acompanhamento fonoaudiológico foi concluído   
        safe_cast(fisio_ok as int64) as fisio_concluido,                              -- Acompanhamento fisioterapêutico realizado/concluído
        case trim(safe_cast(fisio_ok as string))
            when '1' then 'Realizado/Concluído'
            when '0' then 'Pendente'
            when '' then 'Não informado'
            when null then 'Não informado'
            else 'Não classificado'
        end as descricao_fisio_concluido,                                         -- Descrição se o acompanhamento fisioterapêutico foi concluído
        safe_cast( {{process_null('fisio_motora')}} as string) as fisio_motora,                            -- Detalhes/status da fisioterapia motora
        safe_cast( {{process_null('fisio_respiratoria')}} as string) as fisio_respiratoria,                -- Detalhes/status da fisioterapia respiratória
        safe_cast(fisio_ok_motora as int64) as fisio_motora_concluida,                -- Fisioterapia motora realizada/concluída
        case trim(safe_cast(fisio_ok_motora as string))
            when '1' then 'Realizada/Concluída'
            when '0' then 'Pendente'
            when '' then 'Não informado'
            when null then 'Não informado'
            else 'Não classificado'
        end as descricao_fisio_motora_concluida,                                   -- Descrição se a fisioterapia motora foi concluída
        safe_cast( {{process_null('avisos_farm')}} as string) as avisos_farmacia,                          -- Avisos para a farmácia
        safe_cast( {{process_null('estatura')}} as string) as altura_paciente,                             -- Estatura do paciente
        safe_cast( {{process_null('sc')}} as string) as superficie_corporal,                               -- Superfície corporal
        safe_cast( {{process_null('tht')}} as string) as terapia_hormonal_tireoide,                        -- Terapia hormonal da tireoide (não utilizado)
        safe_cast( {{process_null('med')}} as string) as medicacao_especifica,                             -- Medicação específica (não utilizado)
        safe_cast( {{process_null('hv')}} as string) as hidratacao_venosa,                                 -- Hidratação venosa (não utilizado)
        safe_cast( {{process_null('dieta')}} as string) as dieta_prescrita,                                -- Descrição da dieta do paciente
        safe_cast( {{process_null('vm')}} as string) as ventilacao_mecanica,                               -- Ventilação mecânica (não utilizado)
        safe_cast( {{process_null('paren_sedacao')}} as string) as sedacao_parental,                       -- Informações sobre sedação parental
        safe_cast( {{process_null('diag')}} as string) as diagnostico_medico,                              -- Diagnóstico principal
        safe_cast( {{process_null('ig')}} as string) as idade_gestacional,                                 -- Idade gestacional
        safe_cast( {{process_null('igc')}} as string) as idade_gestacional_corrigida,                      -- Idade gestacional corrigida
        safe_cast(sequs as int64) as id_unidade_saude,                                -- Identificador sequencial da unidade de saúde
        _airbyte_extracted_at as loaded_at,
        current_timestamp() as transformed_at
    from {{ source('brutos_prontuario_carioca_saude_mental_prescricao_staging', 'fa_atendimento') }}
    qualify row_number() over (partition by id_atendimento, id_paciente, id_unidade_saude order by _airbyte_extracted_at desc) = 1
)

select 
        {{
            dbt_utils.generate_surrogate_key(
                    [
                        "id_atendimento",
                        "id_paciente",
                        "id_unidade_saude"
                    ]
                )
            }} as id_hci,
        *
from source