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
        select
            *,
            case
                when
                    estoque_movimento_tipo = "TRANSFERENCIA ENTRADA"
                    or estoque_movimento_tipo = "TRANSFERENCIA SAIDA"
                then "Transferência Interna"
                when
                    estoque_movimento_tipo = "INVENTARIO ENTRADA"
                    or estoque_movimento_tipo = "INVENTARIO SAIDA"
                then "Ajuste de Inventário"
                when
                    estoque_movimento_tipo = "AJUSTE ENTRADA"
                    or estoque_movimento_tipo = "AJUSTE SAIDA"
                then "Ajuste na Entrada/Saída"
                when estoque_movimento_tipo = "PERDA"
                then "Avaria / Vencimento"
                else initcap(estoque_movimento_tipo)
            end as estoque_movimento_tipo_grupo,
            case
                when
                    estoque_movimento_tipo = "AJUSTE SAIDA"
                    or estoque_movimento_tipo = "CONSUMO"
                    or estoque_movimento_tipo = "INVENTARIO SAIDA"
                    or estoque_movimento_tipo = "PERDA"
                    or estoque_movimento_tipo = "SAIDA"
                    or estoque_movimento_tipo = "TRANSFERENCIA SAIDA"
                then - material_valor_total
                else material_valor_total
            end as material_valor_total_com_sinal,
            "vitai" as sistema_origem
        from {{ ref("raw_prontuario_vitai__estoque_movimento") }}
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
    estoque_movimento_tipo_grupo as movimento_tipo_grupo,
    estoque_movimento_justificativa as movimento_justificativa,
    estoque_movimento_data as data_evento,
    material_descricao,
    material_quantidade,
    material_valor_total,
    material_valor_total_com_sinal,

    -- Metadata
    sistema_origem,
    data_particao,
    data_carga

from movimento_vitai
