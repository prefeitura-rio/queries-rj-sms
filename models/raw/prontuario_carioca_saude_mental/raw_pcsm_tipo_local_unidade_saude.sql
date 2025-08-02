{{
    config(
        schema="brutos_prontuario_carioca_saude_mental",
        alias="tipo_local_unidade_saude",
        materialized="table",
        tags=["raw", "pcsm", "tipo_local_unidade_saude"],
        description="Tipos possíveis de local. Exemplo: Oficina de artesanato."
    )
}}

select
    safe_cast(seqtplocal as int64) as id_tipo_local,
    safe_cast(nmlocal as string) as nome_tipo_local,
    _airbyte_extracted_at as loaded_at,
    current_timestamp() as transformed_at
from {{ source('brutos_prontuario_carioca_saude_mental_staging', 'gh_tplocal') }}