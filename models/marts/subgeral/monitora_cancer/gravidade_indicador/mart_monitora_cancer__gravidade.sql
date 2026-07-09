-- noqa: disable=LT08

{{
  config(
    schema="projeto_monitora_cancer",
    alias="gravidade",
    partition_by={
        "field": "cpf_particao",
        "data_type": "int64",
        "range": {"start": 0, "end": 100000000000, "interval": 34722222},
},
cluster_by = ["cpf_particao"],
on_schema_change = "sync_all_columns"
)
}}

select
    cpf_particao,
    gravidade_total,
    gravidade_total_0_100,
    gravidade_base,
    gravidade_termo_max,
    gravidade_termo_soma,
    gestante,
    gravidade_detalhamento
from {{ ref("int_monitora_cancer__gravidade") }}
