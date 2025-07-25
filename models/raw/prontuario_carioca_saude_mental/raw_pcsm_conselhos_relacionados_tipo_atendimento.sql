{{
    config(
        schema="brutos_prontuario_carioca_saude_mental",
        alias="conselhos_relacionados_tipo_atendimento",
        materialized="table",
        tags=["raw", "pcsm", "conselhos_relacionados", "tipo_atendimento"],
        description="Lista de conselhos profissionais relacionados a cada tipo de atendimento em saúde mental."
    )
}}

with 
    conselhos_relacionados as 
    (
        select seqtpatend as id_tipo_atendimento, _airbyte_extracted_at as loaded_at, b as conselho 
        from {{ source('brutos_prontuario_carioca_saude_mental_staging', 'gh_tpatendimentos') }},
        unnest(split(dsccatatend, ',')) as b
    )

select a.id_tipo_atendimento, a.conselho, a.loaded_at, current_timestamp() as transformed_at
from conselhos_relacionados a
where trim(ifnull(a.conselho, '')) <> ''