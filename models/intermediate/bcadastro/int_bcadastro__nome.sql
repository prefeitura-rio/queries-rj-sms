{{
    config(
        alias="nome",
        materialized="table",
        schema="intermediario_bcadastro",
        cluster_by="primeiro_nome",
        partition_by={
            "field": "hash_particao",
            "data_type": "int64",
            "range": {
                "start": -9223372036854775808,
                "end": 9223372036854775807,
                "interval": 4613993014934856
            },
        },
    )
}}

with source as (
    select
        tb.cpf,
        tb.nome,
        split(tb.nome, " ")[safe_offset(0)] as primeiro_nome
    from {{ ref("raw_bcadastro__cpf") }} as tb
)

select
    cpf,
    nome,
    primeiro_nome,
    farm_fingerprint(primeiro_nome) as hash_particao
from source
