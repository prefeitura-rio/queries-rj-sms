{{
    config(
        schema="brutos_prontuario_carioca_saude_mental",
        alias="tipos_atendimentos_relacionados",
        materialized="table",
        tags=["raw", "pcsm", "tipos_atendimentos_relacionados"],
        description="Lista de tipos de atendimentos que podem ser utilizados juntos em saúde mental."
    )
}}

with 
    tipos_atendimentos_relacionados as 
    (
        select seqtpatend as id_tipo_atendimento, _airbyte_extracted_at as loaded_at, b as id_tipo_relacionado 
        from {{ source('brutos_prontuario_carioca_saude_mental_staging', 'gh_tpatendimentos') }},
        unnest(split(dsclstatendagrup, ',')) as b
    )

select a.id_tipo_atendimento, a.id_tipo_relacionado, a.loaded_at, current_timestamp() as transformed_at
from tipos_atendimentos_relacionados a
where trim(ifnull(a.id_tipo_relacionado, '')) <> ''