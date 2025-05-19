{{
    config(
        materialized="view",
        enabled=true,
        schema="brutos_sisreg_api",
        alias="devolvidos",
        partition_by={
            "field": "particao_data",
            "data_type": "date",
            "granularity": "month",
        },
    )
}}

select *
from {{ ref("raw_sisreg_api__solicitacoes") }}
where solicitacao_status = "SOLICITAÇÃO / DEVOLVIDA / REGULADOR"
