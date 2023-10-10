{{
    config(
        alias="estoque_posicao",
        schema="saude_prontuario_vitai",
        labels={"contains_pii": "no"},
        materialized="incremental",
        partition_by={
            "field": "data_particao",
            "data_type": "date",
            "granularity": "month",
        },
    )
}}

select
    -- Primary Key
    id as id_estoque_posicao,

    -- Foreign Keys
    safe_cast(cnes as string) as id_cnes,
    safe_cast(lote as string) as id_lote,
    safe_cast(regexp_replace(produtocodigo, r'[^0-9]', '') as string) as id_produto,

    -- Logical Info
    safe_cast(secao as string) as estoque_secao,
    safe_cast(descricao as string) as produto_descricao,
    safe_cast(apresentacao as string) as produto_unidade,
    safe_cast(datavencimento as datetime) as lote_data_vencimento,
    safe_cast(saldo as float64) as produto_quantidade,
    safe_cast(valormedio as float64) as produto_valor_unitario,
    safe_cast(valormedio as float64)
    * safe_cast(saldo as float64) as produto_valor_total,

    -- metadata
    safe_cast(data_particao as date) as data_particao,
    safe_cast(datahora as datetime) as data_snapshot,

from {{ source("saude_prontuario_vitai_staging", "estoque_posicao") }}

{% if is_incremental() %}

    where safe_cast(data_particao as date) > (select max(data_particao) from {{ this }})

{% endif %}
