{{
    config(
        alias="cpf",
        materialized="table",
        schema="brutos_bcadastro",
        cluster_by="situacao_cadastral_tipo",
        partition_by={
            "field": "cpf_particao",
            "data_type": "int64",
            "range": {"start": 0, "end": 100000000000, "interval": 34722222},
        },
    )
}}

select 
    * except(airbyte, metadados), 
    metadados.ano_exercicio 
    from {{ source("brutos_bcadastro", 'cpf') }}