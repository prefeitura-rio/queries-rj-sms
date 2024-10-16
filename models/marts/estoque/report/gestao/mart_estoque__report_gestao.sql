{{
    config(
        alias="report_gestao",
        schema="projeto_estoque",
        materialized="table",
        tag=["report", "daily"],
    )
}}


with
    -- - sources
    medicamentos as (
        select *
        from {{ ref("dim_material") }}
        where
            remume_indicador = 'sim'
            and (
                remume_listagem_basico_indicador = "sim"
            -- - or remume_listagem_uso_interno_indicador = "sim"
            )
    ),  # TODO: revisar critério de seleção de medicamentos

    substitutos as (
        select "65050803616" as id_material, "65050826403" as id_material_substituto,
        union all
        select "65050826403" as id_material, "65050803616" as id_material_substituto
    ),

    posicao as (
        select *
        from {{ ref("mart_estoque__posicao_atual") }}
        inner join medicamentos using (id_material)
        where
            lote_status_padronizado in ("Ativo")
            and estabelecimento_tipo_sms_agrupado in ("APS", "TPC")
            and not (
                sistema_origem = "vitacare" and estoque_secao_caf_indicador = "Não"
            )
    ),

    posicao_aps_sumarized as (
        select
            id_material,
            id_cnes,
            estabelecimento_area_programatica,
            sum(material_quantidade) as material_quantidade,
            avg(material_consumo_medio) as material_consumo_medio
        from posicao
        where estabelecimento_tipo_sms_agrupado = "APS"
        group by id_material, id_cnes, estabelecimento_area_programatica
    ),

    curva_pqrs as (
        select *
        from {{ ref("int_estoque__material_curva_pqrs") }}
        where tipo_sms_agrupado = "APS"
    ),

    registro_preco as (
        select * from {{ ref("raw_sheets__compras_atas_processos_vigentes") }}
    ),

    -- - transformations
    -- dias de estoque
    cmd as (
        select id_material, sum(material_consumo_medio) as cmd
        from posicao_aps_sumarized
        group by id_material
    ),

    posicao_pivoted as (
        select
            p.id_material,
            coalesce(p.aps, 0) as qtd_aps,
            coalesce(p.tpc, 0) as qtd_tpc,
            cmd.cmd,
        from
            (
                select
                    id_material, estabelecimento_tipo_sms_agrupado, material_quantidade
                from posicao
            ) pivot (
                sum(material_quantidade) for estabelecimento_tipo_sms_agrupado
                in ("APS", "TPC")
            ) as p
        left join cmd using (id_material)
    ),

    -- zeradas
    -- - unidades zeradas
    ubs_zeradas as (
        select
            id_material,
            count(
                distinct case when material_quantidade = 0 then id_cnes else null end
            ) as zerados_ubs,
        from posicao_aps_sumarized
        group by id_material
        order by zerados_ubs
    ),

    -- - areas programáticas zeradas
    ap_consolidado as (
        select
            id_material,
            estabelecimento_area_programatica as ap,
            sum(material_quantidade) as qtd,
            sum(material_consumo_medio) as cmd,
            {{
                dbt_utils.safe_divide(
                    "sum(material_quantidade)", "sum(material_consumo_medio)"
                )
            }} as cobertura,
        from posicao_aps_sumarized
        group by id_material, estabelecimento_area_programatica
        order by id_material, estabelecimento_area_programatica
    ),

    ap_zeradas as (
        select
            id_material,
            count(distinct case when qtd < 1 then ap else null end) as zeradas_ap,
        from ap_consolidado
        group by id_material
    ),

    -- - final query
    sources_joined as (
        select
            m.id_material,
            m.nome,
            coalesce(pqrs.pqrs_categoria, "S") as pqrs_categoria,  -- itens sem qualquer movimento não são classificados
            round(pqrs.usuarios_atendidos_mes, 0) as usuarios_atendidos_mes,
            m.hierarquia_n1_categoria,
            m.hierarquia_n2_subcategoria,
            m.cadastrado_sistema_vitacare_indicador,
            m.ativo_indicador,
            m.abastecimento_responsavel,
            m.abastecimento_frequencia,
            coalesce(p.qtd_aps, 0) as qtd_aps,
            coalesce(p.qtd_tpc, 0) as qtd_tpc,
            coalesce(round(p.cmd, 2), 0) as cmd,
            m.farmacia_popular_disponibilidade_indicador,
            coalesce(rp.rp_vigente_indicador, "nao") as rp_vigente_indicador,
            rp.vencimento_data,
            concat(
                upper(substr(status, 1, 1)), lower(substr(status, 2))
            ) as registro_preco_status,
        from medicamentos as m
        left join posicao_pivoted as p using (id_material)
        left join curva_pqrs as pqrs using (id_material)
        left join registro_preco as rp using (id_material)
    ),

    sources_com_cobertura as (
        select
            s.*,
            if(s.qtd_aps = 0, 10, za.zeradas_ap) as zeradas_ap,
            if(
                s.qtd_aps = 0,
                (select count(distinct id_cnes) from posicao_aps_sumarized),
                zu.zerados_ubs
            ) as zerados_ubs,  -- correção para incluir unidades com estoques positivos porém vencidos
            coalesce(
                round({{ dbt_utils.safe_divide("s.qtd_aps", "s.cmd") }}, 2),
                if(cmd is null, null, 0)
            ) as cobertura_aps_dias,
            coalesce(
                round({{ dbt_utils.safe_divide("s.qtd_tpc", "s.cmd") }}, 2),
                if(cmd is null, null, 0)
            ) as cobertura_tpc_dias,
            coalesce(
                round({{ dbt_utils.safe_divide("s.qtd_aps + s.qtd_tpc", "s.cmd") }}, 2),
                if(cmd is null, null, 0)
            ) as cobertura_total_dias
        from sources_joined as s
        left join ubs_zeradas as zu using (id_material)
        left join ap_zeradas as za using (id_material)
    ),

    -- - status e motivo_status
    sources_joined_com_cobertura_e_status as (
        select
            *,
            case
                when
                    farmacia_popular_disponibilidade_indicador = "nao"
                    and registro_preco_status not in ("Suspenso pela anvisa")
                then
                    (
                        case
                            when
                                rp_vigente_indicador = "nao"
                                and cobertura_total_dias <= 90
                            then
                                case
                                    when
                                        (
                                            contains_substr(
                                                registro_preco_status, "homolog"
                                            )
                                            and cobertura_total_dias > 40
                                        )
                                        or (
                                            contains_substr(
                                                registro_preco_status, "analise"
                                            )
                                            and cobertura_total_dias > 60
                                        )
                                    then ""
                                    else "Sem RP, menos de 90 dias de estoque"
                                end
                            when
                                rp_vigente_indicador = "sim"
                                and cobertura_total_dias <= 30
                            then "Com RP, menos de 30 dias de estoque"
                            else ""
                        end
                    )
                else ""
            end as status
        from sources_com_cobertura
    ),

    -- corrige acentuação
    sources_acentuacao_corrigada as (
        select

            * except (
                farmacia_popular_disponibilidade_indicador,
                rp_vigente_indicador,
                registro_preco_status
            ),

            if(
                farmacia_popular_disponibilidade_indicador = "nao", "não", "sim"
            ) as farmacia_popular_disponibilidade_indicador,

            if(rp_vigente_indicador = "nao", "não", "sim") as rp_vigente_indicador,

            case
                when registro_preco_status = "Em analise"
                then "Em análise"
                when registro_preco_status = "Aguardando publicacao"
                then "Aguardando publicação"
                when registro_preco_status = "Processo na gl"
                then "Processo na GL"
                when registro_preco_status = ""
                then null
                else registro_preco_status
            end as registro_preco_status,

        from sources_joined_com_cobertura_e_status
    ),

    -- indica os registros que vão ser exibidos
    source_exibidos as (
        select
            *,
            if(
                status != ""
                and (
                    pqrs_categoria in ('P', 'Q')
                    or id_material in ("65051001688", "65051000878", "65051001840")  # TODO: revisar regra
                ),
                'sim',
                'nao'
            ) as exibir_registro_indicador
        from sources_acentuacao_corrigada
    ),

    -- final
    final as (
        select
            id_material,
            nome,
            pqrs_categoria,
            usuarios_atendidos_mes,
            hierarquia_n1_categoria,
            hierarquia_n2_subcategoria,
            cadastrado_sistema_vitacare_indicador,
            abastecimento_frequencia,
            qtd_aps,
            qtd_tpc,
            cmd,
            cobertura_aps_dias,
            cobertura_tpc_dias,
            cobertura_total_dias,
            zeradas_ap,
            zerados_ubs,
            ativo_indicador,
            farmacia_popular_disponibilidade_indicador,
            abastecimento_responsavel,
            rp_vigente_indicador,
            vencimento_data,
            registro_preco_status,
            status,
            exibir_registro_indicador
        from source_exibidos
        where
            (hierarquia_n1_categoria = "Medicamento" or id_material = "65058200201")
            and ativo_indicador = "sim"
        order by cobertura_total_dias asc, status, nome asc
    )

select *
from final
