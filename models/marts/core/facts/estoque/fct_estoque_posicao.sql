{{
    config(
        alias="posicao",
        schema="saude_estoque",
        labels = {
            "dominio": "estoque",
            "dado_publico": "nao",
            "dado_pessoal": "nao",
            "dado_anonimizado": "nao",
            "dado_sensivel_saude": "nao"
        },
        materialized="table",
        partition_by={
            "field": "data_particao",
            "data_type": "date",
            "granularity": "month",
        },
    )
}}

with
    -- sources
    -- - Vitacare
    vitacare_atual_zerados as (
        select * from {{ ref("int_estoque__posicao_hoje_vitacare_zerados") }}
    ),
    vitacare_posicao_historico as (
        select *
        from {{ ref("raw_prontuario_vitacare__estoque_posicao") }}
    ),

    vitacare_completa as (
        select *
        from vitacare_atual_zerados
        union all
        select *
        from vitacare_posicao_historico
    ),

    -- - Vitai
    vitai_atual_zerados as (
        select * from {{ ref("int_estoque__posicao_hoje_vitai_zerados") }}
    ),
    vitai_posicao_historico as (
        select *
        from {{ ref("raw_prontuario_vitai__estoque_posicao") }}
    ),

    vitai_completa as (
        select *
        from vitai_atual_zerados
        union all
        select *
        from vitai_posicao_historico
    ),

    -- - TPC
    tpc_atual_zerados as (
        select * from {{ ref("int_estoque__posicao_hoje_tpc_zerados") }}
    ),

    tpc_posicao_historico as (
        select *
        from {{ ref("raw_estoque_central_tpc__estoque_posicao") }}
    ),


    tpc_completa as (
        select *
        from tpc_atual_zerados
        union all
        select *
        from tpc_posicao_historico
    ),

    -- constroi a posicação para cada source
    posicao_vitacare as (
        select
            estoque.id_cnes,
            estoque.id_lote,
            estoque.id_material,
            "nao" as estoque_reservado_para_abastecimento,
            estoque.armazem as estoque_secao,
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
            estoque.lote_status,
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
            '' as lote_status,
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
            '' as lote_status,
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
            remume_listagem_basico_indicador
            as material_remume_listagem_basico_indicador,
            remume_listagem_uso_interno_indicador
            as material_remume_listagem_uso_interno_indicador,
            remume_listagem_hospitalar_indicador
            as material_remume_listagem_hospitalar_indicador,
            remume_listagem_antiseptico_indicador
            as material_remume_listagem_antiseptico_indicador,
            remume_listagem_estrategico_indicador
            as material_remume_listagem_estrategico_indicador,
        from posicao_consolidada as pos
        left join
            {{ ref("int_estoque__material_relacao_remume_por_estabelecimento") }}
            as remume
            on pos.id_cnes = remume.id_cnes
            and pos.id_material = remume.id_material

    ),

    final as (
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
            lower({{ clean_name_string("estoque_secao") }}) as estoque_secao,
            estoque_reservado_para_abastecimento,
            if(lote_status = "", null, lower({{ clean_name_string("lote_status") }})) as lote_status,
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
    )

select * from final

