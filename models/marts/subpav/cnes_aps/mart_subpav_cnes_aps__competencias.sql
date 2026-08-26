{{
    config(
        materialized = 'table',
        alias        = "competencias",
        tags         = ["subpav", "cnes_aps"],
        cluster_by   = ["competencia_id"]
    )
}}

select
    competencia_id,
    competencia_id as id,
    competencia as ds_competencia,
    competencia,
    data_particao,
    dt_final_competencia as dt_final,
    base_final,
    created_at,
    updated_at

from {{ ref('int_subpav_cnes_aps__competencias_legado') }}
