{{
    config(
        schema="brutos_prontuario_carioca_saude_mental",
        alias="local_unidade_saude",
        materialized="table",
        tags=["raw", "pcsm", "local_unidade_saude"],
        description="Locais de unidades de saúde. Um local é uma edificação pertencente a uma unidade."
    )
}}

select
    safe_cast(seqlocal as int64) as id_local,
    safe_cast(nmlocal as string) as descricao_local,
    safe_cast(endlocal as string) as endereco_local,
    safe_cast(seqtplocal as int64) as id_tipo_local,
    safe_cast(sequs as int64) as id_unidade_saude,
    _airbyte_extracted_at as loaded_at,
    current_timestamp() as transformed_at
from {{ source('brutos_prontuario_carioca_saude_mental_staging', 'gh_local') }}