{{
    config(
        schema="brutos_prontuario_carioca_saude_mental",
        alias="equipes",
        materialized="table",
        tags=["raw", "pcsm", "equipes"],
        description="Equipes em que estão organizados os profissionais de saúde mental da prefeitura do Rio de Janeiro. Nem todos os profissionais de saúde estão organizados em equipe."
    )
}}

select
    safe_cast(seqequipe as int64) as id_equipe,
    safe_cast(sequs as int64) as id_unidade_saude,
    safe_cast(dscequipe as string) as descricao_equipe,
    safe_cast(tpequipe as string) as tipo_equipe,
    case trim(safe_cast(tpequipe as string))
        when 'D' then 'Deambulatório'
        when 'M' then 'Mini equipe'
        when '' then 'Não informado'
        when null then 'Não informado'
        else 'Não classificado'
    end as descricao_tipo_equipe,
    _airbyte_extracted_at as loaded_at,
    current_timestamp() as transformed_at
from {{ source('brutos_prontuario_carioca_saude_mental_staging', 'gh_equipe') }}