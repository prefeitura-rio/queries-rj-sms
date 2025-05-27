{{
    config(
        materialized="view",
        enabled=true,
        schema="brutos_sisreg_api",
        alias="autorizados",
        partition_by={
            "field": "particao_data",
            "data_type": "date",
            "granularity": "month",
        },
    )
}}


select *
from {{ ref("raw_sisreg_api__marcacoes") }}
where solicitacao_status = "SOLICITAÇÃO / AUTORIZADA / REGULADOR"
