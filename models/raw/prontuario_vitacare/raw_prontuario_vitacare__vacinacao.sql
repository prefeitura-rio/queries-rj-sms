{{
    config(
        alias="vacinacao",
        partition_by={
            "field": "particao_data_vacinacao",
            "data_type": "date",
            "granularity": "month",
        }
    )
}}

with
    source as (
        select * from {{ source("brutos_prontuario_vitacare_api_centralizadora", "vacinacao_historico") }}
        union all
        select * from {{ ref("raw_prontuario_vitacare_api_centralizadora__vacinacao") }}
    )

select *
from source
qualify row_number() over(
    partition by id_vacinacao
    order by metadados.updated_at, metadados.loaded_at desc
) = 1

