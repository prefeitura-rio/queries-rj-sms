{{
    config(
        alias="sumario",
        schema="app_historico_clinico_treinamento",
        materialized="table",
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
    ['Cetoprofeno','Plasil'] as allergies,
    ['Atenolol','Captopril','Gliclazida'] as continuous_use_medications

