{{
    config(
        alias="busca",
        schema="app_historico_clinico_treinamento",
        materialized="table",
        cluster_by="nome",
        partition_by={
            "field": "cpf_particao",
            "data_type": "int64",
            "range": {"start": 0, "end": 100000000000, "interval": 34722222},
        },
    )
}}

select 
    42298037299 as cpf_particao,
    '42298037299' as cpf,
    ['700000000000000'] as cns_lista,
    'Paciente Fake I' as nome,
    null as nome_social,
    '1989-01-01' as data_nascimento,
    80 as idade,
    'masculino' as genero,
    'Mãe Fake I' as nome_mae,
    10 as quantidade_episodios,
    struct(
        true as indicador,
        [] as motivos,
        ['31'] as ap_cadastro,
        ['6664040'] as unidades_cadastro
    ) as exibicao
