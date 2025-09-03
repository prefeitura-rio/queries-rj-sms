-- solicitacoes que estão aguardando resposta de algum regulador
{{
    config(
        materialized="view",
        enabled=false,
        schema="brutos_sisreg_api",
        alias="executados",
        partition_by={
            "field": "particao_data",
            "data_type": "date",
            "granularity": "month",
        },
    )
}}
select *
from {{ ref("raw_sisreg_api__marcacoes") }}
where
    solicitacao_status in (
        "AGENDAMENTO / CONFIRMADO / EXECUTANTE",  -- paciente compareceu
        "AGENDAMENTO / FALTA / EXECUTANTE",  -- paciente faltou
        "AGENDAMENTO / PENDENTE CONFIRMAÇÃO / EXECUTANTE"  -- não sabemos se o paciente compareceu ou não
    )
