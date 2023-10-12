{{
    config(
        alias="estoque_movimento",
        schema="brutos_prontuario_vitai",
        labels={"contains_pii": "no"},
        materialized="view",
        partition_by={
            "field": "data_particao",
            "data_type": "date",
            "granularity": "month",
        },
    )
}}

select
    -- Primary Key

    -- Foreign Keys
    safe_cast(cnes as string) as id_cnes,
    safe_cast(
        regexp_replace(produtocodigo, r'[^a-zA-Z0-9]', '') as string
    ) as id_produto,

    -- Logical Info
    safe_cast(descricao as string) as material_descricao,
    safe_cast(apresentacao as string) as material_unidade,
    safe_cast(secaoOrigem as string) as estoque_secao_origem,
    safe_cast(secaoDestino as string) as estoque_secao_destino,
    safe_cast(tipoMovimento as string) as estoque_movimento_tipo,
    safe_cast(justificativaMovimentacao as string) as estoque_movimento_justificativa,
    safe_cast(safe_cast(dataMovimentacao as datetime) as date) as estoque_movimento_data,  
    safe_cast(quantidade as float64) as material_quantidade,
    safe_cast(valor as float64) as material_valor_total,


    -- metadata
    safe_cast(data_particao as date) as data_particao,
    safe_cast(_data_carga as datetime) as data_carga,

from {{ source("brutos_prontuario_vitai_staging", "estoque_movimento") }}

{% if is_incremental() %}

    where safe_cast(data_particao as date) > (select max(data_particao) from {{ this }})

{% endif %}