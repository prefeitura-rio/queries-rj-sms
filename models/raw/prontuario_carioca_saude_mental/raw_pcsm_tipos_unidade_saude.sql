{{
    config(
        schema="brutos_prontuario_carioca_saude_mental",
        alias="tipos_unidade_saude",
        materialized="table",
        tags=["raw", "pcsm", "tipos_unidade_saude"],
        description="Tipos de unidades de saúde."
    )
}}

select
    safe_cast(seqtipous as int64) as id_tipo_unidade_saude,
    safe_cast(dsctipous as string) as descricao_tipo_unidade_saude,
    safe_cast(indclasstu as string) as classificacao_tipo_unidade_saude,
    safe_cast(dscclassresum as string) as classe_resumida,
    safe_cast(dscclassdetal as string) as classe_detalhada,
    _airbyte_extracted_at as loaded_at, 
    current_timestamp() as transformed_at
from {{ source('brutos_prontuario_carioca_saude_mental_staging', 'gh_us_tipo') }}