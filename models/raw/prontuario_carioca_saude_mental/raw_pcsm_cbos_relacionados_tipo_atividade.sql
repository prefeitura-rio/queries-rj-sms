{{
    config(
        schema="brutos_prontuario_carioca_saude_mental",
        alias="cbos_relacionados_tipo_atividade",
        materialized="table",
        tags=["raw", "pcsm", "cbos_relacionados", "tipo_atendimento"],
        description="Lista de CBOs relacionados a cada tipo de atendimento em saúde mental."
    )
}}

with 
    cbos_relacionados as 
    (
        select seqtpativ as id_tipo_atividade, _airbyte_extracted_at as loaded_at, b as cbo 
        from {{ source('brutos_prontuario_carioca_saude_mental_staging', 'gh_tpatividades') }},
        unnest(split(listcbo, ',')) as b
    )

select a.id_tipo_atividade, a.cbo, a.loaded_at, current_timestamp() as transformed_at
from cbos_relacionados a
where trim(ifnull(a.cbo, '')) <> ''