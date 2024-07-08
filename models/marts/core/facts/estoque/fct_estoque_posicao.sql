{{
    config(
        alias="posicao",
        schema="saude_estoque",
        partition_by={
            "field": "data_particao",
            "data_type": "date",
            "granularity": "month",
        },
        enabled=false
    )
}}

with
    -- sources
    --- Vitacare
    vitacare_atual as (
        select * from {{ ref("int_estoque__posicao_hoje_vitacare_com_zerados_remume") }}
    ),
    vitacare_dias_anteriores as (
        select *
        from {{ ref("raw_prontuario_vitacare__estoque_posicao") }}
        where data_particao < current_date('America/Sao_Paulo')
    ),

    vitacare_completa as (
        select *
        from vitacare_atual
        union all
        select *
        from vitacare_dias_anteriores
    ),

    --- Vitai
    vitai_atual as (
        select * from {{ ref("int_estoque__posicao_hoje_vitai_com_zerados_remume") }}
    ),
    vitai_dias_anteriores as (
        select *
        from {{ ref("raw_prontuario_vitai__estoque_posicao") }}
        where data_particao < current_date('America/Sao_Paulo')
    ),

    vitai_completa as (
        select *
        from vitai_atual
        union all
        select *
        from vitai_dias_anteriores
    ),

    --- TPC
    particao_mais_recente as (
        select max(data_particao) as data_particao
        from {{ ref("raw_estoque_central_tpc__estoque_posicao") }}
    ),

    tpc_atual as (
        select * from {{ ref("int_estoque__posicao_hoje_tpc_com_zerados_remume") }}
    ),

    tpc_dias_anteriores as (
        select *
        from {{ ref("raw_estoque_central_tpc__estoque_posicao") }}
        where
            data_particao < date_sub(
                (select data_particao from particao_mais_recente), interval 1 day
            )
    ),

    tpc_completa as (
        select *
        from tpc_atual
        union all
        select *
        from tpc_dias_anteriores
    ),

    -- constroi a posicação para cada source
    posicao_vitacare as (
        select
            estoque.id_cnes,
            estoque.id_lote,
            estoque.id_material,
            "nao" as estoque_reservado_para_abastecimento,
            "" as estoque_secao,
            estoque.material_descricao,
            "" as material_unidade,  -- payload da viticare não possui esta informação
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
        from vitacare_completa as estoque
        left join {{ ref("dim_estabelecimento") }} as estabelecimento using (id_cnes)
        left join
            {{ ref("int_estoque__material_valor_unitario_tpc") }} as valor_unitario
            using (id_material)
    ),

    posicao_vitai as (
        select
            estoque.id_cnes,
            estoque.id_lote,
            estoque.id_material,
            "nao" as estoque_reservado_para_abastecimento,
            estoque.estoque_secao,
            estoque.material_descricao,
            estoque.material_unidade,
            estoque.lote_data_vencimento,
            estoque.material_quantidade,
            estoque.material_valor_unitario,
            estoque.material_valor_total,
            estoque.data_particao,
            estoque.data_snapshot,
            estoque.data_carga,
            "vitai" as sistema_origem,
            estabelecimento.tipo as estabelecimento_tipo,
            estabelecimento.tipo_sms as estabelecimento_tipo_sms,
            estabelecimento.area_programatica as estabelecimento_area_programatica,
        from vitai_completa as estoque
        left join {{ ref("dim_estabelecimento") }} as estabelecimento using (id_cnes)
    ),

    posicao_tpc as (
        select
            "tpc" as id_cnes,
            *,
            "tpc" as sistema_origem,
            "ESTOQUE CENTRAL" as estabelecimento_tipo,
            "ESTOQUE CENTRAL" as estabelecimento_tipo_sms,
            "TPC" as estabelecimento_area_programatica,
        from tpc_completa
    ),

    posicao_consolidada as (
        select *
        from posicao_vitai
        union all
        select *
        from posicao_tpc
        union all
        select *
        from posicao_vitacare
    ),

    posicao_consolidada_com_remume as (
        select
            pos.*,
            if(remume.id_material is null, "nao", "sim") as material_remume_indicador,
            remume_listagem_basico_indicador as material_remume_listagem_basico_indicador,
            remume_listagem_uso_interno_indicador as material_remume_listagem_uso_interno_indicador,
            remume_listagem_hospitalar_indicador as material_remume_listagem_hospitalar_indicador,
            remume_listagem_antiseptico_indicador as material_remume_listagem_antiseptico_indicador,
            remume_listagem_estrategico_indicador as material_remume_listagem_estrategico_indicador,
        from posicao_consolidada as pos
        left join
            {{ ref("int_estoque__material_relacao_remume_por_estabelecimento") }}
            as remume
            on pos.id_cnes = remume.id_cnes
            and pos.id_material = remume.id_material

    )

select
    -- Primary Key
    -- Foreign Keys
    id_cnes,
    id_material,
    id_lote,
    concat(id_cnes, "-", id_material) as id_cnes_material,
    case
        when id_cnes = 'tpc'  -- TPC
        then "-"
        when estabelecimento_tipo = 'CENTRO DE SAUDE/UNIDADE BASICA'
        then concat("ap-", estabelecimento_area_programatica, "-", id_material)
        when estabelecimento_tipo <> 'CENTRO DE SAUDE/UNIDADE BASICA'
        then concat("cnes-", id_cnes, "-", id_material)
        else "-"
    end as id_curva_abc,

    -- Common Fields
    material_descricao,
    material_unidade,
    estoque_secao,
    estoque_reservado_para_abastecimento,
    lote_data_vencimento,
    material_quantidade,
    material_valor_unitario,
    material_valor_total,
    material_remume_indicador,
    material_remume_listagem_basico_indicador,
    material_remume_listagem_uso_interno_indicador,
    material_remume_listagem_hospitalar_indicador,
    material_remume_listagem_antiseptico_indicador,
    material_remume_listagem_estrategico_indicador,
    
    -- Metadata
    sistema_origem,
    data_particao,
    data_snapshot,
    data_carga,
from posicao_consolidada_com_remume