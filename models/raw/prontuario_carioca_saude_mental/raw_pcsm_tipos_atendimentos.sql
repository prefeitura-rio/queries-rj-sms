{{
    config(
        schema="brutos_prontuario_carioca_saude_mental",
        alias="tipos_atendimentos",
        materialized="table",
        tags=["raw", "pcsm", "tipos_atendimentos"],
        description="Tipos de atendimentos oferecidos para saúde mental pela prefeitura do Rio de Janeiro."
    )
}}

select
    safe_cast(seqtpatend as int64) as id_tipo_atendimento,
    safe_cast(indclass as string) as classificacao_atendimento,
    case trim(safe_cast(indclass as string))
        when 'AM' THEN 'DEAMBULATÓRIO'
        when 'CA' then 'CAPS'
        when 'EM' then 'EMERGÊNCIA'
        when 'HP' then 'INTERNAÇÃO'
        when '' then 'Não informado'
        when null then 'Não informado'
        else 'Não classificado'
    end as descricao_classificacao_atendimento,
    safe_cast(descatend as string) as descricao_tipo_atendimento,
    safe_cast(indacaosave as string) as acao_pos_atendimento,
    case trim(safe_cast(indacaosave as string))
        when 'S' then 'Sim'
        when 'N' then 'Não'
        when '' then 'Não informado'
        when null then 'Não informado'
        else 'Não classificado'
    end as descricao_acao_pos_atendimento,
    _airbyte_extracted_at as loaded_at,
    current_timestamp() as transformed_at
from {{ source('brutos_prontuario_carioca_saude_mental_staging', 'gh_tpatendimentos') }}