{{
    config(
        alias="estoque_posicao",
        schema="brutos_prontuario_vitai",
        labels={"contains_pii": "no"},
        partition_by={
            "field": "data_particao",
            "data_type": "date",
            "granularity": "month",
        },
        enabled=false
    )
}}
-- source
with
    source as (
        select * from {{ source("brutos_prontuario_vitai_staging", "estoque_posicao") }}
    ),

    -- fix mudança no formato dos campos de data de Date para Datetime
    source_2023 as (
        select
            *,
            safe_cast(
                safe_cast(datavencimento as datetime) as date
            ) as lote_data_vencimento,
            safe_cast(datahora as datetime) as data_snapshot,
        from source
        where data_particao <= '2023-12-31'
    ),

    source_2024 as (
        select
            *,
            safe_cast(
                datetime(
                    parse_timestamp('%Y-%m-%dT%H:%M:%E*S%Ez', datavencimento),
                    'UTC-03:00'
                ) as date
            ) as lote_data_vencimento,
            safe_cast(
                datetime(
                    parse_timestamp('%Y-%m-%dT%H:%M:%E*S%Ez', datahora),
                    'UTC-03:00'
                ) as datetime
            ) as data_snapshot,
        from source
        where data_particao > '2023-12-31'
    ),

    source_merged as (
        select *
        from source_2023
        union all
        select *
        from source_2024
    )

select
    -- Primary Key
    -- Foreign Keys
    safe_cast(cnes as string) as id_cnes,
    safe_cast(lote as string) as id_lote,
    safe_cast(
        regexp_replace(produtocodigo, r'[^a-zA-Z0-9]', '') as string
    ) as id_material,

    -- Logical Info
    safe_cast(secao as string) as estoque_secao,
    safe_cast(descricao as string) as material_descricao,
    safe_cast(apresentacao as string) as material_unidade,
    safe_cast(lote_data_vencimento as date) as lote_data_vencimento,
    safe_cast(saldo as float64) as material_quantidade,
    safe_cast(valormedio as float64) as material_valor_unitario,
    safe_cast(valormedio as float64)
    * safe_cast(saldo as float64) as material_valor_total,

    -- metadata
    safe_cast(data_particao as date) as data_particao,
    data_snapshot,
    safe_cast(_data_carga as datetime) as data_carga,

from source_merged

where
    cnes <> "2970619"  -- Centro Carioca dos Olhos
    
    {% if is_incremental() %}

        and safe_cast(data_particao as date)
        > (select max(data_particao) from {{ this }})

    {% endif %}
