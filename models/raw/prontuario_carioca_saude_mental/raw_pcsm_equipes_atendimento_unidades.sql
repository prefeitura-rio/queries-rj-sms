{{
    config(
        schema="brutos_prontuario_carioca_saude_mental",
        alias="equipes_atendimento_unidades",
        materialized="table",
        tags=["raw", "pcsm", "equipes"],
        description="Lista com o sequencial das unidades de saúde ambuatoriais que podem encaminhar o atendimento para esta equipe separados por vírgula."
    )
}}

with 
    equipes_atendimento_unidades as 
    (
    select seqequipe as id_equipe, _airbyte_extracted_at as loaded_at, b as id_unidade_saude 
    from {{ source('brutos_prontuario_carioca_saude_mental_staging', 'gh_equipe') }},
    unnest(split(dsclstusatend, ',')) as b
    )

select a.id_equipe, a.id_unidade_saude, a.loaded_at, current_timestamp() as transformed_at
from equipes_atendimento_unidades a
where trim(ifnull(a.id_unidade_saude, '')) <> ''