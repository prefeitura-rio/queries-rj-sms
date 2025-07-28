{{
    config(
        schema="brutos_prontuario_carioca_saude_mental",
        alias="tipos_encaminhamentos",
        materialized="table",
        tags=["raw", "pcsm", "tipos_encaminhamentos"],
        description="Tipos de encaminhamentos dos atendimentos."
    )
}}

select
    safe_cast(seqenatend as int64) as id_tipo_encaminhamento,
    safe_cast(descencaminha as string) as descricao_encaminhamento,
    safe_cast(indstatus as string) as tipo_atendimento,
    case trim(safe_cast(indstatus as string))
        when 'D' then 'Deambulatorio'
        when 'C' then 'CAPS'
        when 'T' then 'Transferência'
        when '' then 'Não informado'
        when null then 'Não informado'
        else 'Tipo de atendimento não classificado'
    end as descricao_tipo_atendimento,
    safe_cast(indinativo as string) as encaminhamento_inativo,
    case trim(safe_cast(indinativo as string))
        when 'S' then 'Inativo'
        when 'N' then 'Ativo'
        when '' then 'Ativo'
        when null then 'Ativo'
        else 'Status de inativo não classificado'
    end as descricao_status_encaminhamento_ativo,
    safe_cast(indtemdest as string) as atendimento_encaminhado,
    case trim(safe_cast(indtemdest as string))
        when 'S' then 'Sim'
        when 'N' then 'Não'
        when '' then 'Não informado'
        when null then 'Não informado'
        else 'Status de encaminhamento não classificado'
    end as descricao_atendimento_encaminhado,
    safe_cast(indclasstu as string) as classificacao_encaminhamento,
    case trim(safe_cast(indclasstu as string))
        when 'AM' then 'Deambulatório'
        when 'AP' then 'Atenção Primária'
        when 'CA' then 'CAPS'
        when 'EM' then 'Emergência Psiquiátrica'
        when 'HP' then 'Internação Psiquiátrica'
        when 'HE' then 'Internação / Emergência'
        when 'HO' then 'Hosp. Outros Municípios'
        when 'CO' then 'CAPS Outros Municípios'
        when 'PL' then 'Policlínica'
        when 'CR' then 'Reabilitação'
        when 'NC' then 'Nível Central'
        when 'EG' then 'Emergência Geral'
        when 'HG' then 'Hospital Geral'
        when 'HZ' then 'Hospital Especializado'
        when 'HC' then 'Hospital de Custódia'
        when 'HU' then 'Hospital Universitário'
        when 'IN' then 'Institutos'
        when '' then 'Não informado'
        when null then 'Não informado'
        else 'Classificação não identificada'
    end as descricao_classificacao_encaminhamento,
    safe_cast(indformorig as string) as tipo_encaminhamento,
    case trim(safe_cast(indformorig as string))
        when 'F' then 'Facultativo'
        when 'N' then 'Não informar'
        when '' then 'Obrigatório'
        when null then 'Obrigatório'
        else 'Tipo de encaminhamento não classificado'
    end as descricao_tipo_encaminhamento,   
    _airbyte_extracted_at as loaded_at,
    current_timestamp() as transformed_at
from {{ source('brutos_prontuario_carioca_saude_mental_staging', 'gh_encam_atend') }}