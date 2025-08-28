{{
    config(
        alias="estoque_posicao_historico",
        tags="vitacare_estoque",
        labels={
            "dominio": "estoque",
            "dado_publico": "nao",
            "dado_pessoal": "nao",
            "dado_anonimizado": "nao",
            "dado_sensivel_saude": "nao",
        },
        partition_by={
            "field": "data_particao",
            "data_type": "date",
            "granularity": "day",
        },
    )
}}

with estoque_posicao_historico as (
    select 
        data, 
        _source_cnes, 
        _source_ap, 
        safe_cast(_target_date as string) as _target_date, 
        _endpoint,
        _loaded_at,
        extract(year from safe_cast(_loaded_at as timestamp)) as ano_particao,
        extract(month from safe_cast(_loaded_at as timestamp)) as mes_particao,
        safe_cast(safe_cast(_loaded_at as timestamp) as date) as data_particao
    from {{ source("brutos_prontuario_vitacare_api_centralizadora_staging", "estoque_posicao_historico") }}
)

select * from estoque_posicao_historico
