-- solicitacoes que estão aguardando resposta de algum regulador
{{
    config(
        materialized="view",
        enabled=true,
        schema="brutos_sisreg_api",
        alias="fila_regulacao",
        partition_by={
            "field": "particao_data",
            "data_type": "date",
            "granularity": "month",
        },
    )
}}

select *
from {{ ref("raw_sisreg_api__solicitacoes") }}
where
    solicitacao_status in (
        "SOLICITAÇÃO / PENDENTE / REGULADOR",
        "SOLICITAÇÃO / PENDENTE / FILA DE ESPERA",

        -- solicitacoes que foram devolvidas anteriormente e agora foram reenviadas
        "SOLICITAÇÃO / REENVIADA / REGULADOR"
    )
