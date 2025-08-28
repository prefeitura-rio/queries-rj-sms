{{
    config(
        alias="estoque_movimento_historico",
        tags="vitacare_estoque",
        labels={
            "dominio": "estoque",
            "dado_publico": "nao",
            "dado_pessoal": "nao",
            "dado_anonimizado": "nao",
            "dado_sensivel_saude": "nao",
        },
    )
}}

with estoque_movimento_historico as (
    select 
        data, 
        _source_cnes, 
        _source_ap, 
        safe_cast(_target_date as string) as _target_date, 
        _endpoint,
        _loaded_at,
        safe_cast(extract(year from safe_cast(_loaded_at as timestamp)) as string) as ano_particao,
        safe_cast(extract(month from safe_cast(_loaded_at as timestamp)) as string) as mes_particao,
        substr(_loaded_at, 1, 10) as data_particao
    from {{ source("brutos_prontuario_vitacare_api_centralizadora_staging", "estoque_movimento_historico") }}
)

select * from estoque_movimento_historico
