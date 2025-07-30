{{
    config(
        schema="brutos_prescricao",
        alias="unidades_saude",
        materialized="table",
        tags=["raw", "pcsm", "unidades_saude"],
        description="Instalações físicas ou setores específicos de atendimento à saúde, presentes em todos os hospitais e clínicas da rede municipal do Rio de Janeiro."
    )
}}

select
    safe_cast(sequs as int64) as id_unidade_saude,                -- Identificador sequencial único da Unidade de Saúde (US)
    safe_cast(dscus as string) as nome_unidade_saude,             -- Descrição ou nome da Unidade de Saúde (US)
    safe_cast(codcnes as string) as cnes_unidade_saude,           -- Código CNES da unidade
    safe_cast(sequssm as int64) as sequencial_unidade_saude,      -- Sequencial da Unidade de Saúde ou do sistema de saúde mental
    _airbyte_extracted_at as loaded_at,
    current_timestamp() as transformed_at
from {{ source('brutos_prescricao_staging', 'fa_us') }}