{{
    config(
        materialized="view",
        enabled=false,
        schema="brutos_sisreg_api",
        alias="negados",
        partition_by={
            "field": "particao_data",
            "data_type": "date",
            "granularity": "month",
        },
    )
}}

select *
from {{ ref("raw_sisreg_api__solicitacoes") }}
where solicitacao_status = "SOLICITAÇÃO / NEGADA / REGULADOR"
