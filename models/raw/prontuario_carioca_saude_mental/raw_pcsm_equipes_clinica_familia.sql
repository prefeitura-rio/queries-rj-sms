{{
    config(
        schema="brutos_prontuario_carioca_saude_mental",
        alias="equipes_clinica_familia",
        materialized="table",
        tags=["raw", "pcsm", "equipes_clinica_familia"],
        description="Equipes de clínicas de família."
    )
}}

select
    safe_cast(sequs as int64) as id_unidade_saude,                -- Identificador da Unidade Atenção Primária à qual a equipe está associada
    safe_cast(seqequip as int64) as id_equipe,                    -- Identificador único da equipe
    safe_cast(dscequip as string) as descricao_equipe,            -- Descrição da equipe
    safe_cast(indativo as string) as status_ativo,                -- Indicador de status (S-Sim/N-Não)
    case trim(safe_cast(indativo as string))
        when 'S' then 'Ativo'
        when 'N' then 'Inativo'
        when '' then 'Não informado'
        when null then 'Não informado'
        else 'Não classificado'
    end as descricao_status_ativo,                                -- Descrição do status da equipe
    _airbyte_extracted_at as loaded_at,
    current_timestamp() as transformed_at
from {{ source('brutos_prontuario_carioca_saude_mental_staging', 'gh_clinfamil_equipes') }}