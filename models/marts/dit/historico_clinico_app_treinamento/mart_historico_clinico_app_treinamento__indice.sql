{{
    config(
        alias="indice",
        schema="app_historico_clinico_treinamento",
        materialized="table",
        cluster_by="nome",
        partition_by={
            "field": "cns_particao",
            "data_type": "int64",
            "range": {"start": 0, "end": 1000000000000000, "interval": 333333333334},
        },
    )
}}

select 
    42298037299 as cns_particao,
    '42298037299' as cpf,
    'Paciente Fake I' as nome

