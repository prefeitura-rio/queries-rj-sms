{{
    config(
        schema="brutos_prontuario_carioca_saude_mental_prescricao",
        alias="medicos",
        materialized="table",
        tags=["raw", "pcsm", "medicos"],
        description="Profissionais de saúde atuantes nos diversos setores e especialidades dos hospitais e unidades de atendimento da rede municipal do Rio de Janeiro."
    )
}}

select
    safe_cast(crm as int64) as numero_crm,                           -- Número do Conselho Regional de Medicina (CRM)
    safe_cast(nome as string) as nome_medico,                        -- Nome completo do médico
    safe_cast(ativo as int64) as cadastro_ativo,                     -- Indica se o cadastro do médico está ativo (1) ou inativo (0)
    case trim(safe_cast(ativo as string))
        when '1' then 'Ativo'
        when '0' then 'Inativo'
        when '' then 'Não informado'
        when null then 'Não informado'
        else 'Não classificado'
    end as descricao_cadastro_ativo,                                 -- Descrição do status do cadastro do médico
    safe_cast(cpf as string) as cpf_medico,                          -- CPF do médico
    safe_cast(crm_uf as string) as unidade_federativa_crm,           -- UF de registro do CRM do médico
    _airbyte_extracted_at as loaded_at,
    current_timestamp() as transformed_at
from {{ source('brutos_prontuario_carioca_saude_mental_prescricao_staging', 'fa_medico') }}