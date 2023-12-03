{{
    config(
        alias="estoque_posicao",
        schema="saude_estoque",
        partition_by={
            "field": "data_particao",
            "data_type": "date",
            "granularity": "month",
        },
    )
}}

# TODO: subtituir .tipo por .tipo

with

    remume as (select * from {{ ref('int_estoque__material_relacao_remume_por_estabelecimento') }}),
    
    -- importar a posicao vitacare
    -- juntar posicao remume com seu respectivo prontuario
    -- ajustar os campos nullos da remume na tabela de posicao
    -- re-apontar o posicao_vitacare_final para a tabela de posicao ajustada


    posicao_vitacare_final as (
        select
            estoque.id_cnes,
            estoque.id_lote,
            estoque.id_material,
            "" as estoque_secao,
            estoque.material_descricao,
            "" as material_unidade, --payload da viticare não possui esta informação
            estoque.lote_data_vencimento,
            estoque.material_quantidade,
            if(
                valor_unitario.material_valor_unitario_medio is null,
                0,
                valor_unitario.material_valor_unitario_medio
            ) as material_valor_unitario,
            estoque.material_quantidade * if(
                valor_unitario.material_valor_unitario_medio is null,
                0,
                valor_unitario.material_valor_unitario_medio
            ) as material_valor_total,
            estoque.data_particao,
            safe_cast(estoque.data_particao as datetime) as data_snapshot,
            estoque.data_carga,
            "vitacare" as sistema_origem,
            estabelecimento.tipo as estabelecimento_tipo,
            estabelecimento.tipo_sms as estabelecimento_tipo_sms,
            estabelecimento.area_programatica as estabelecimento_area_programatica,
        from {{ ref("raw_prontuario_vitacare__estoque_posicao") }} as estoque
        left join {{ ref("dim_estabelecimento") }} as estabelecimento using (id_cnes)
        left join
            {{ ref("int_estoque__material_valor_unitario_tpc") }}
            as valor_unitario using (id_material)
    ),

    posicao_vitai as (
        select
            estoque.*,
            "vitai" as sistema_origem,
            estabelecimento.tipo as estabelecimento_tipo,
            estabelecimento.tipo_sms as estabelecimento_tipo_sms,
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
            "ESTOQUE CENTRAL" as estabelecimento_tipo_sms,
            "-" as estabelecimento_area_programatica,
        from {{ ref("raw_estoque_central_tpc__estoque_posicao") }}
    ),

    posicao_consolidada as (
        select *
        from posicao_vitai
        union all
        select *
        from posicao_tpc
        union all
        select *
        from posicao_vitacare_final
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
