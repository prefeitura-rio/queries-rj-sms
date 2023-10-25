{{
    config(
        alias="estoque_movimento",
        schema="saude_estoque",
        labels={"contains_pii": "no"},
        materialized="view",
        partition_by={
            "field": "data_particao",
            "data_type": "date",
            "granularity": "month",
        },
    )
}}

with
    movimento_vitai as (
        select *, "vitai" as sistema_origem
        from {{ ref("brutos_prontuario_vitai__estoque_movimento") }}
    )

select
    -- Primary Key
    -- Foreing Key
    id_cnes,
    id_material,

    -- Common Fields
    estoque_secao_origem as localizacao_origem,
    estoque_secao_destino as localizacao_destino,
    estoque_movimento_tipo as movimento_tipo,
    estoque_movimento_justificativa as movimento_justificativa,
    estoque_movimento_data as data_evento,
    material_quantidade,
    material_valor_total,

    -- Metadata
    sistema_origem,
    data_particao,
    data_carga

from movimento_vitai
