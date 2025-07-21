{{
    config(
        alias="estoque_posicao_atual",
        schema="projeto_estoque",
        materialized="table",
    )
}}


with
    -- source
    posicao as (
        select *, concat(id_cnes, "-", data_particao) as id_estabelecimento_particao
        from {{ ref("fct_estoque_posicao") }}
    ),

    posicao_atual as (
        select posicao.*
        from posicao
        left join
            {{ ref("int_estoque__posicao_mais_recente_por_estabelecimento") }}
            as pos_atual using (id_estabelecimento_particao)
        where pos_atual.id_estabelecimento_particao is not null
    ),  -- para toda unidade pegamos o registro mais recente para evitar falta de dados de alguma unidade

    remume as (
        select *
        from {{ ref("int_estoque__material_relacao_remume_por_estabelecimento") }}
    ),

    sigma as (select distinct * from {{ source("brutos_sigma_staging", "material") }}),  -- existe um bug na origem do sigma gerando duplicidade

    historico_dispensacao as (
        select *
        from {{ ref("int_estoque__dispensacao_agrupada_por_unidade_abc_e_material") }}
    ),

    curva_abc as (select * from {{ ref("int_estoque__material_curva_abc") }}),

    dispensacao_media as (
        select * from {{ ref("int_estoque__dispensacao_media_mensal") }}
    ),

    material as (select * from {{ ref("dim_material") }}),

    estabelecimento as (select * from {{ ref("dim_estabelecimento") }}),

    posicao_final as (
        select
            pos.*,
            if(
                sistema_origem <> "tpc", est.tipo, "ESTOQUE CENTRAL"
            ) as estabelecimento_tipo_cnes,
            if(
                sistema_origem <> "tpc", est.tipo_sms, "ESTOQUE CENTRAL"
            ) as estabelecimento_tipo_sms,
            if(
                sistema_origem <> "tpc", est.tipo_sms_agrupado, "TPC"
            ) as estabelecimento_tipo_sms_agrupado,
            if(
                sistema_origem <> "tpc", est.area_programatica, "TPC"
            ) as estabelecimento_area_programatica,
            if(
                sistema_origem <> "tpc", est.nome_limpo, "TPC"
            ) as estabelecimento_nome_limpo,
            if(
                sistema_origem <> "tpc", est.nome_sigla, "TPC"
            ) as estabelecimento_nome_sigla,
            if(
                sistema_origem <> "tpc", est.administracao, "direta"
            ) as estabelecimento_administracao,
            if(
                sistema_origem <> "tpc", est.responsavel_sms, "subpav"
            ) as estabelecimento_responsavel_sms,
            if(
                sistema_origem <> "tpc",
                coalesce(cmm.cmd_com_outliers, 0),
                cmm.cmd_com_outliers
            ) as material_consumo_medio,
            coalesce(abc.abc_categoria, "S/C") as abc_categoria,
            coalesce(
                mat.nome,
                concat(sig.nm_padronizado, " ", nm_complementar_material),
                pos.material_descricao
            ) as material_descricao_full,
            if(mat.nome is null, "nao", "sim") as material_cadastro_esta_correto,
            case
                when abc.abc_categoria is not null
                then '-'
                when abc.abc_categoria is null and sistema_origem = 'tpc'
                then "ABC não calculado para TPC"
                when abc.abc_categoria is null and pos.material_valor_total = 0
                then 'item não possui preço cadastrado'
                when abc.abc_categoria is null and disp.id_curva_abc is null
                then
                    "item não possui histórico de dispensação registrado na unidade/região"
                else 'desconhecida'
            end as abc_justificativa_ausencia,
            case
                when cmm.cmd_com_outliers is not null
                then '-'
                when cmm.cmd_com_outliers is null and sistema_origem = 'tpc'
                then "CMM não calculado para TPC"
                when cmm.cmd_com_outliers is null and disp.id_curva_abc is null
                then "item não possui histórico de dispensação registrado na unidade"
                else 'desconhecida'
            end as cmm_justificativa_ausencia,
            est.tipo_sms_simplificado as estabelecimento_tipo_sms_simplificado,
            mat.hierarquia_n1_categoria as material_hierarquia_n1_categoria,
            mat.hierarquia_n2_subcategoria as material_hierarquia_n2_subcategoria,
            mat.controlado_indicador as material_controlado_indicador,
            mat.controlado_tipo as material_controlado_tipo,
        from posicao_atual as pos  -- posicao_atual
        left join estabelecimento as est on (pos.id_cnes = est.id_cnes)
        left join curva_abc as abc using (id_curva_abc)
        left join historico_dispensacao as disp using (id_curva_abc)
        left join
            dispensacao_media as cmm
            on (pos.id_material = cmm.id_material and pos.id_cnes = cmm.id_cnes)
        left join material as mat on (pos.id_material = mat.id_material)
        left join sigma as sig on (pos.id_material = sig.cd_material)
        where est.prontuario_estoque_tem_dado = "sim" or pos.id_cnes = "tpc"
    ),

    final as (

        select
            -- Foreign keys
            id_cnes,
            id_curva_abc,
            id_material,
            id_cnes_material,

            -- Common fields
            estabelecimento_tipo_cnes,
            estabelecimento_tipo_sms,
            estabelecimento_tipo_sms_simplificado,
            estabelecimento_tipo_sms_agrupado,
            estabelecimento_area_programatica,
            estabelecimento_nome_limpo,
            estabelecimento_nome_sigla,
            estabelecimento_administracao,
            estabelecimento_responsavel_sms,
            abc_categoria as material_abc_categoria,
            material_hierarquia_n1_categoria,
            material_hierarquia_n2_subcategoria,
            material_remume_indicador,
            material_remume_listagem_basico_indicador,
            material_remume_listagem_uso_interno_indicador,
            material_remume_listagem_hospitalar_indicador,
            material_remume_listagem_estrategico_indicador,
            material_controlado_indicador,
            material_controlado_tipo,
            material_descricao_full as material_descricao,
            material_unidade,
            material_cadastro_esta_correto,
            {{ proper_br("estoque_secao") }} as estoque_secao,
            case
                when sistema_origem = "vitacare" and estoque_secao = "farmacia geral"
                then "Sim"
                when sistema_origem = "vitacare" and estoque_secao <> "farmacia geral"
                then "Não"
                when id_cnes = "6927254"  -- newton bethlen, enquanto não chegam dados com o novo payload  # TODO: retirar após atualizacao
                then "Sim"
                when sistema_origem = "tpc"
                then "Sim"
                else "Desconhecido"
            end as estoque_secao_caf_indicador,
            id_lote,
            lote_data_vencimento,
            lote_status,
            case
                -- Ocasionalmente recebemos lotes já vencidos com status de removido em
                -- quantias >0; isso faz com que, p.ex., o BI mostre o lote como 'vencido'
                -- mesmo que o lote não exista na vida real (pois já foi removido)
                -- Portanto, precisamos conferir primeiro o status de removido, e só depois
                -- a data de validade
                when lote_status = "removed"
                then "Removido"
                when current_date('America/Sao_Paulo') > lote_data_vencimento
                then "Vencido"
                -- "Suspenso" aqui é somente outra forma de estoque, não significa indisponível
                when lote_status = "suspended"
                then "Suspenso"
                when lote_status = "active" or lote_status = "recovered"
                then "Ativo"
                else "Ativo"
            end as lote_status_padronizado,
            if(
                current_date('America/Sao_Paulo') > lote_data_vencimento, "nao", "sim"
            ) as lote_validade_dentro_indicador,
            date_diff(
                lote_data_vencimento, current_date('America/Sao_Paulo'), day
            ) as lote_dias_para_vencer,
            material_quantidade,
            material_valor_unitario,
            material_valor_total,
            material_consumo_medio,
            if(
                material_quantidade = 0,
                0,
                {{
                    dbt_utils.safe_divide(
                        "material_quantidade", "material_consumo_medio"
                    )
                }}
            ) as estoque_cobertura_dias,
            abc_justificativa_ausencia,
            cmm_justificativa_ausencia,

            -- Metadata 
            concat(
                id_material,
                " - ",
                upper(material_descricao_full),
                " - ",
                lower(material_descricao_full),
                " - ",
                initcap(material_descricao_full)
            ) as busca_material_id_descricao_case_insensitive,
            sistema_origem,
            data_particao,
            date_diff(
                current_date('America/Sao_Paulo'), data_particao, day
            ) as dias_desde_ultima_atualizacao,
            data_carga,

        from posicao_final
        order by
            estabelecimento_tipo_sms_agrupado,
            estabelecimento_area_programatica,
            estabelecimento_nome_limpo,
            material_descricao
    )

select *
from final
where sistema_origem <> "vitai"  -- retirado por falta de dados 
