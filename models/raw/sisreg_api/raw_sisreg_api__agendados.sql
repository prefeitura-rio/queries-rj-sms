-- solicitacoes que estão aguardando resposta de algum regulador
config(
    materialized = "view",
    enabled = true,
    schema = "brutos_sisreg_api",
    alias = "agendados",
    partition_by = {"field":"particao_data", "data_type":"date", "granularity":"month"},
)

select *
from {{ ref("raw_sisreg_api__marcacoes") }}
where
    solicitacao_status in (
        "SOLICITAÇÃO / AGENDADA / COORDENADOR",
        "SOLICITAÇÃO / AGENDADA / SOLICITANTE"
    )
