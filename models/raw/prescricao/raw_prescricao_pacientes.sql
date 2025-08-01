{{
    config(
        schema="brutos_prontuario_carioca_saude_mental_prescricao",
        alias="pacientes",
        materialized="table",
        tags=["raw", "pcsm", "pacientes"],
        description="Indivíduos recebendo assistência e cuidados de saúde em qualquer uma das unidades de atendimento e hospitais da rede municipal do Rio de Janeiro."
    )
}}

select
    safe_cast(id_paciente as int64) as id_paciente,                       -- Identificador único do paciente no sistema
    safe_cast(registro as int64) as registro_prontuario,                  -- Número de registro hospitalar ou prontuário do paciente
    safe_cast(nome as string) as nome_paciente,                           -- Nome completo do paciente
    safe_cast(data_nascimento as date) as data_nascimento,                -- Data de nascimento do paciente
    safe_cast( {{process_null('tipo_documento')}} as string) as tipo_documento,                -- Origem da prescrição: p=Prontuário, b=BE (boletim de emergência), i=Internação
    case trim(lower(safe_cast(tipo_documento as string)))
        when 'p' then 'Prontuário'
        when 'b' then 'BE (Boletim de Emergência)'
        when 'i' then 'Internação'
        when '' then 'Não informado'
        when null then 'Não informado'
        else 'Não classificado'
    end as descricao_tipo_documento,                                      -- Descrição da origem da prescrição
    safe_cast(peso as numeric) as peso_paciente,                          -- Peso do paciente em quilogramas
    safe_cast(estatura as float64) as altura_paciente,                    -- Altura do paciente
    safe_cast(sc as float64) as superficie_corporal,                      -- Superfície corporal do paciente
    safe_cast(dscnmmae as string) as nome_mae_paciente,                   -- Nome completo da mãe do paciente
    safe_cast(dscnmresp as string) as nome_responsavel_legal,             -- Nome completo do responsável legal pelo paciente
    _airbyte_extracted_at as loaded_at,
    current_timestamp() as transformed_at
from {{ source('brutos_prontuario_carioca_saude_mental_prescricao_staging', 'fa_paciente') }}