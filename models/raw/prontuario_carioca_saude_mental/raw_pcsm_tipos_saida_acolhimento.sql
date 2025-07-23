{{
    config(
        schema="brutos_prontuario_carioca_saude_mental",
        alias="tipos_saida_acolhimento",
        materialized="table",
        tags=["raw", "pcsm", "tipos_saida_acolhimento"],
        description="Tipos possíveis de saída de acolhimento."
    )
}}

select
    safe_cast(seqtpsaida as int64) as id_tipo_saida_acolhimento,
    safe_cast(dsctpsaida as string) as descricao_tipo_saida_acolhimento,
    _airbyte_extracted_at as loaded_at,
    current_timestamp() as transformed_at
from {{ source('brutos_prontuario_carioca_saude_mental_staging', 'gh_tpsaidaacolhe') }}