{{
    config(
        schema="brutos_prontuario_carioca_saude_mental",
        alias="atividades_grupo",
        materialized="table",
        tags=["raw", "pcsm", "atividades_grupo"],
        description="Atividades de grupo realizadas em unidades de saúde mental (psicossociais) do município do Rio de Janeiro."
    )
}}

select
    safe_cast(seqativgrp as int64) as id_atividade_grupo,
    safe_cast(nmativgrp as string) as nome_atividade_grupo,
    safe_cast(dtativ as date) as data_inicio_atividade,
    safe_cast({{ process_null('horaativ') }} as string) as hora_inicio_atividade,
    safe_cast({{ process_null('horaativfim') }} as string) as hora_termino_atividade,
    safe_cast(seqlocal as int64) as local_atividade_grupo,
    safe_cast(seqtpativ as int64) as id_tipo_atividade_grupo,
    safe_cast(dtativfim as date) as data_termino_atividade,
    safe_cast(obsmatri as string) as observacao_atividade,
    safe_cast(sequs as int64) as id_unidade_saude,
    safe_cast(dscperiodativ as string) as periodo_atividade_grupo,
    _airbyte_extracted_at as loaded_at,
    current_timestamp() as transformed_at
from {{ source('brutos_prontuario_carioca_saude_mental_staging', 'gh_ativgrp') }}