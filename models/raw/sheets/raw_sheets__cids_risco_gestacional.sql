{{
    config(
        schema="brutos_sheets",
        alias="cids_risco_gestacional",
        tags=["daily", "subgeral", "cnes_subgeral", "monitora_reg"],
    )
}}
-- TODO: conferir tags acima

with
    source as (
        select
            categoria,
            cid,
            descricao,
            categoria_de_risco_temporal as categoria_risco_temporal,
            janela_de_relevancia as janela_relevancia,
            encaminhamento_alto_risco,
            alto_risco_gestacao_anterior,
            alto_risco_gestacao_atual,
            justificativa__condicao as justificativa_condicao
        from {{ source("brutos_sheets_staging", "cids_risco_gestacional") }}
    )

select *
from source
