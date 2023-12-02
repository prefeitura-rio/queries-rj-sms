{{
    config(
        alias="estoque_posicao",
        schema="brutos_estoque_central_tpc",
        materialized="table",
        partition_by={
            "field": "data_particao",
            "data_type": "date",
            "granularity": "day",
        },
    )
}}

select
    -- Primary Key

    -- Foreign Keys
    safe_cast(id_lote as string) as id_lote,
    safe_cast(
        regexp_replace(cod_depositante, r'[^a-zA-Z0-9]', '') as string
    ) as id_material,

    -- Logical Info
    safe_cast(local_nome as string) as estoque_secao,
    safe_cast(item_nome_longo as string) as material_descricao,
    safe_cast(unidade as string) as material_unidade,
    safe_cast(safe_cast(validade as datetime) as date) as lote_data_vencimento,
    safe_cast(qtd_dispo as float64) as material_quantidade,
    safe_cast(preco_unitario as float64) as material_valor_unitario,
    safe_cast(preco_unitario as float64)
    * safe_cast(qtd_dispo as float64) as material_valor_total,

    -- metadata
    safe_cast(data_particao as date) as data_particao,
    safe_cast(data_atualizacao as datetime) as data_snapshot,
    safe_cast(_data_carga as datetime) as data_carga,

from {{ source("brutos_estoque_central_tpc_staging", "estoque_posicao") }}

{% if is_incremental() %}

    where safe_cast(data_particao as date) > (select max(data_particao) from {{ this }})

{% endif %}
