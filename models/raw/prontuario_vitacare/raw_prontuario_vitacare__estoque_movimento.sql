{{
    config(
        alias="estoque_movimento",
        labels={
            "dominio": "estoque",
            "dado_publico": "nao",
            "dado_pessoal": "sim",
            "dado_anonimizado": "nao",
            "dado_sensivel_saude": "sim",
        },
        partition_by={
            "field": "particao_data_movimento",
            "data_type": "date",
            "granularity": "month",
        },
        tags=['daily','vitacare_estoque']
    )
}}

with
    source as (
        select * from {{ source("brutos_prontuario_vitacare_api_centralizadora", "estoque_movimento_historico") }}
        union all
        select * from {{ ref("raw_prontuario_vitacare_api_centralizadora__estoque_movimento") }}
    )

select *
from source
qualify row_number() over(
    partition by id_surrogate
    order by metadados.updated_at desc
) = 1

