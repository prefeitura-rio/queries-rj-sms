-- solicitacoes que estão aguardando resposta de algum regulador
{{
    config(
        materialized="view",
        enabled=false,
        schema="saude_sisreg",
        alias="agendados",
        partition_by={
            "field": "data_solicitacao",
            "data_type": "date",
            "granularity": "month",
        },
    )
}}

select *
from {{ ref("raw_sisreg_api__marcacoes") }}
where
    solicitacao_status in (
        "SOLICITAÇÃO / AGENDADA / COORDENADOR",
        "SOLICITAÇÃO / AGENDADA / SOLICITANTE"
    )
