{{
    config(
        alias="estoque_monitoramento_historico",
        schema="projeto_estoque",
        materialized="incremental",
    )
}}

select * from {{ ref('mart_estoque__monitoramento') }}

{% if is_incremental() %}

    where data_snapshot > (select max(data_snapshot) from {{ this }})

{% endif %}