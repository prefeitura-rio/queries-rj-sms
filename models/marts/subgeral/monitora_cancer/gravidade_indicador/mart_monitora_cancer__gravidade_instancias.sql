-- noqa: disable=LT08

{{
  config(
    schema="projeto_monitora_cancer",
    alias="gravidade_instancias",
    partition_by={
        "field": "cpf_particao",
        "data_type": "int64",
        "range": {"start": 0, "end": 100000000000, "interval": 34722222},
    },
    cluster_by=["criterio"],
    on_schema_change="sync_all_columns",
  )
}}

select
    cpf_particao,
    criterio,
    etapa,
    data_trigger,
    dias_atraso,
    intervalo_urgencia_dias,
    risco_evento_gatilho,
    peso_criterio,
    fator_tempo,
    fator_risco,
    gravidade_criterio,
    gestante
from {{ ref("int_monitora_cancer__gravidade_instancias") }}
