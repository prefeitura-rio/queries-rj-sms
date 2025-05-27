{{
    config(
        materialized="view",
        enabled=true,
        schema="brutos_sisreg_api",
        alias="cancelados",
        partition_by={
            "field": "particao_data",
            "data_type": "date",
            "granularity": "month",
        },
    )
}}

select
    split(solicitacao_status, " / ")[0] as cancelamento_etapa,
    split(solicitacao_status, " / ")[2] as cancelamento_autor,
    *

from {{ ref("raw_sisreg_api__solicitacoes") }}
where
    solicitacao_status in (
        -- solicitacoes que foram canceladas
        "SOLICITAÇÃO / CANCELADA / SOLICITANTE",
        "SOLICITAÇÃO / CANCELADA / REGULADOR",
        "SOLICITAÇÃO / CANCELADA / COORDENADOR",

        -- agendamentos que foram cancelados
        "AGENDAMENTO / CANCELADO / COORDENADOR",
        "AGENDAMENTO / CANCELADO / REGULADOR",
        "AGENDAMENTO / CANCELADO / SOLICITANTE"
    )
