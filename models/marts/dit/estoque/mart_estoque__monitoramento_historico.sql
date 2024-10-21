{{
    config(
        enabled=false,
        alias="estoque_monitoramento_historico",
        schema="projeto_estoque",
        materialized="incremental",
    )
}}

select * from {{ ref('mart_estoque__monitoramento') }}

{% if is_incremental() %}

    where data_referencia > (select max(data_referencia) from {{ this }})

{% endif %}