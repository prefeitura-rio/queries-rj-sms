{{
    config(
        alias="estoque_posicao",
        schema="saude_estoque",
        materialized="table",
        partition_by={
            "field": "data_particao",
            "data_type": "date",
            "granularity": "month",
        },
    )
}}

with
    posicao_vitai as (
        select
            estoque.*,
            "vitai" as sistema_origem,
            estabelecimento.tipo as estabelecimento_tipo,
            estabelecimento.area_programatica as estabelecimento_area_programatica,
        from {{ ref("raw_prontuario_vitai__estoque_posicao") }} as estoque
        left join {{ ref("dim_estabelecimento") }} as estabelecimento using (id_cnes)
    ),

    posicao_tpc as (
        select
            "-" as id_cnes,
            *,
            "tpc" as sistema_origem,
            "ESTOQUE CENTRAL" as estabelecimento_tipo,
            "-" as estabelecimento_area_programatica,
        from {{ ref("raw_estoque_central_tpc__estoque_posicao") }}
    ),

    posicao_consolidada as (
        select *
        from posicao_vitai
        union all
        select *
        from posicao_tpc
    )

select
    -- Primary Key
    -- Foreign Keys
    id_cnes,
    id_lote,
    id_material,
    concat(id_cnes, "-", id_material) as id_cnes_material,
    case
        when id_cnes = '-'  -- TPC
        then "-"
        when estabelecimento_tipo = 'CENTRO DE SAUDE/UNIDADE BASICA'
        then concat("ap-", estabelecimento_area_programatica, "-", id_material)
        when estabelecimento_tipo <> 'CENTRO DE SAUDE/UNIDADE BASICA'
        then concat("cnes-", id_cnes, "-", id_material)
        else "-"
    end as id_curva_abc,

    -- Common Fields
    estoque_secao,
    material_descricao,
    material_unidade,
    lote_data_vencimento,
    material_quantidade,
    material_valor_unitario,
    material_valor_total,

    -- Metadata
    sistema_origem,
    data_particao,
    data_snapshot,
    data_carga,
from posicao_consolidada
