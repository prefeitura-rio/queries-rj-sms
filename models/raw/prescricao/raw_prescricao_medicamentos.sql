{{
    config(
        schema="brutos_prontuario_carioca_saude_mental_prescricao",
        alias="medicamentos",
        materialized="table",
        tags=["raw", "pcsm", "medicamentos"],
        description="Produtos farmacêuticos disponíveis para dispensação e uso nos hospitais e clínicas da rede municipal de saúde do Rio de Janeiro."
    )
}}

select
    safe_cast(id_medicamento as int64) as id_medicamento,                         -- Identificador único para cada medicamento
    safe_cast(nome as string) as nome_medicamento,                                -- Nome comercial do medicamento
    safe_cast(via as string) as via_administracao,                                -- Via de administração do medicamento
    safe_cast(dose as numeric) as dose_administrada,                              -- Quantidade do medicamento administrada em cada tomada
    safe_cast(intervalo as string) as intervalo_doses,                            -- Tempo entre as doses do medicamento
    safe_cast(previsao as string) as previsao_consumo,                            -- Previsão de consumo do medicamento, em dias
    safe_cast(justificativa as string) as justificativa_prescricao,               -- Justificativa para a prescrição do medicamento
    safe_cast(observacoes as string) as observacao_administracao,                 -- Observações relevantes sobre a administração
    safe_cast(id_prescricao as int64) as id_prescricao,                           -- Identificador único da prescrição médica
    safe_cast(qtd_atendida as numeric) as quantidade_atendida,                    -- Quantidade do medicamento atendida na dispensação
    safe_cast(cod_med_hospub as string) as codigo_medicamento,                    -- Código do medicamento no sistema do hospital
    safe_cast(cpf_farmaceutico as string) as cpf_farmaceutico,                    -- CPF do farmacêutico que dispensou o medicamento
    safe_cast(nome_farmaceutico as string) as nome_farmaceutico,                  -- Nome completo do farmacêutico que dispensou o medicamento
    safe_cast(apresentacao as string) as apresentacao_medicamento,                -- Apresentação comercial do medicamento
    safe_cast(dose_tot as string) as dose_total_prescrita,                        -- Dose total do medicamento prescrita
    safe_cast(dias_medic as string) as total_dias_tratamento,                     -- Número total de dias de tratamento
    safe_cast(soro as string) as administrado_soro,                               -- Nome do tipo de soro diluente usado na medicação
    safe_cast(hidratacao as string) as hidratacao_associada,                      -- Indica se há necessidade de hidratação associada
    case trim(safe_cast(hidratacao as string))
        when 'S' then 'Sim'
        when 'N' then 'Não'
        when '' then 'Não informado'
        when null then 'Não informado'
        else 'Não classificado'
    end as descricao_hidratacao_associada,                                                -- Descrição da necessidade de hidratação
    safe_cast(qtd_va as string) as quantidade_estoque,                            -- Quantidade do medicamento em estoque
    safe_cast(medicva as string) as medicamento_disponivel_unidade,               -- Indica se o medicamento está disponível na unidade de saúde
    safe_cast(etiqueta_aprasamento as int64) as etiqueta_aprasamento,             -- Indica se a etiqueta de aprasamento foi aplicada
    safe_cast(numqtdposo2 as numeric) as quantidade_posologia2,                   -- Quantidade do medicamento em segunda posologia
    safe_cast(numqtdposo3 as numeric) as quantidade_posologia3,                   -- Quantidade do medicamento em terceira posologia
    safe_cast(datdispens as date) as data_dispensacao,                            -- Data da dispensação do medicamento
    safe_cast(numqtddispensdia as int64) as quantidade_dispensada_dia,            -- Quantidade dispensada por dia
    _airbyte_extracted_at as loaded_at,
    current_timestamp() as transformed_at
from {{ source('brutos_prontuario_carioca_saude_mental_prescricao_staging', 'fa_medicamento') }}