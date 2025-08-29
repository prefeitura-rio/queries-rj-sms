{{
    config(
        alias="estoque_posicao",
        labels={
            "dominio": "estoque",
            "dado_publico": "nao",
            "dado_pessoal": "nao",
            "dado_anonimizado": "nao",
            "dado_sensivel_saude": "nao",
        },
        partition_by={
            "field": "particao_data_posicao",
            "data_type": "date",
            "granularity": "day",
        },
        tags=['daily','vitacare_estoque']
    )
}}

with
    source as (
        select * from {{ source("brutos_prontuario_vitacare_api_centralizadora_staging", "estoque_posicao_historico") }}
        union all
        select * from {{ ref("raw_prontuario_vitacare_api_centralizadora__estoque_posicao") }}
    )

select *
from source
qualify row_number() over(
    partition by id_surrogate
    order by metadados.updated_at desc
) = 1

