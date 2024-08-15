{{
    config(
        alias="report_gestao",
        schema="projeto_estoque",
        materialized="table",
        tag=["report", "weekly"],
    )
}}


with
    -- - sources
    medicamentos as (
        select *
        from {{ ref("dim_material") }}
        where remume_indicador = 'sim' and remume_listagem_basico_indicador = "sim"
    ),  # TODO: revisar critério de seleção de medicamentos

    posicao as (
        select *
        from {{ ref("mart_estoque__posicao_atual_agregado") }}
        inner join medicamentos using (id_material)
        where estabelecimento_tipo_sms_agrupado in ("APS", "TPC")
    ),

    posicao_aps as (
        select * from posicao where estabelecimento_tipo_sms_agrupado = "APS"
    ),

    curva_pqrs as (
        select *
        from {{ ref("int_estoque__material_curva_pqrs") }}
        where tipo_sms_agrupado = "APS"
    ),

    registro_preco as (select * from {{ ref("raw_sheets__compras_atas_processos_vigentes") }}),

    -- - transformations
    -- dias de estoque
    cmd as (
        select id_material, sum(material_consumo_medio) as cmd
        from posicao_aps
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
        from posicao_aps
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
        from posicao_aps
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
            pqrs.usuarios_atendidos_mes,
            m.hierarquia_n1_categoria,
            m.hierarquia_n2_subcategoria,
            m.cadastrado_sistema_vitacare_indicador,
            p.qtd_aps,
            p.qtd_tpc,
            round(p.cmd, 0) as cmd,
            coalesce(
                round({{ dbt_utils.safe_divide("p.qtd_aps", "p.cmd") }}, 2),
                if(cmd is null, null, 0)
            ) as cobertura_aps_dias,
            coalesce(
                round({{ dbt_utils.safe_divide("p.qtd_tpc", "p.cmd") }}, 2),
                if(cmd is null, null, 0)
            ) as cobertura_tpc_dias,
            coalesce(
                round({{ dbt_utils.safe_divide("p.qtd_aps + p.qtd_tpc", "p.cmd") }}, 2),
                if(cmd is null, null, 0)
            ) as cobertura_total_dias,
            if(p.qtd_aps = 0, 10, za.zeradas_ap) as zeradas_ap,
            if(
                p.qtd_aps = 0,
                (select count(distinct id_cnes) from posicao_aps),
                zu.zerados_ubs
            ) as zerados_ubs,  -- correção para incluir unidades com estoques positivos porém vencidos
            -- zu.zerados_ubs as zerados_ubs_sem_correcao,
            m.farmacia_popular_disponibilidade_indicador,
            coalesce(rp.rp_vigente_indicador, "nao") as rp_vigente_indicador,
            rp.vencimento_data,
            CONCAT(UPPER(SUBSTR(status, 1, 1)), LOWER(SUBSTR(status, 2))) AS registro_preco_status,
        from medicamentos as m
        left join posicao_pivoted as p using (id_material)
        left join ubs_zeradas as zu using (id_material)
        left join ap_zeradas as za using (id_material)
        left join curva_pqrs as pqrs using (id_material)
        left join registro_preco as rp using (id_material)
        order by cobertura_total_dias asc
    )

select *, 
    case
        when cobertura_total_dias is null then "3. Atenção"
        when rp_vigente_indicador = "nao" and cobertura_total_dias = 0 then "1. Muito Crítico"
        when rp_vigente_indicador = "nao" and cobertura_total_dias < 30 then "1. Muito Crítico"
        when rp_vigente_indicador = "nao" and cobertura_total_dias >= 30 then "2. Crítico"
        when rp_vigente_indicador = "sim" and cobertura_total_dias = 0 then "2. Crítico"
        when rp_vigente_indicador = "sim" and cobertura_total_dias > 0 and zeradas_ap > 0 then "3. Atenção"
        when rp_vigente_indicador = "sim" and cobertura_total_dias < 10 then "3. Atenção"
        when rp_vigente_indicador = "sim" and cobertura_total_dias > 0 and zeradas_ap = 0 then "4. Ok"
        else "Verificar"
    end as status,
    case
        when cobertura_total_dias is null then "3.3. Sem histórico de CMD"
        when rp_vigente_indicador = "nao" and cobertura_total_dias = 0 then "1.1. Zerado e sem RP"
        when rp_vigente_indicador = "nao" and cobertura_total_dias < 30 then "1.2. Estoque baixo e sem RP"
        when rp_vigente_indicador = "nao" and cobertura_total_dias >= 30 then "2.2. Estoque alto, mas sem RP"
        when rp_vigente_indicador = "sim" and cobertura_total_dias = 0 then "2.1. Zerado com RP"
        when rp_vigente_indicador = "sim" and cobertura_total_dias > 0 and zeradas_ap > 0 then  "3.2. Estoque desbalanceado: AP(s) zerada(s)"
        when rp_vigente_indicador = "sim" and cobertura_total_dias < 10 then "3.1. Estoque baixo: menos de dez dias na rede"
        when rp_vigente_indicador = "sim" and cobertura_total_dias > 0 and zeradas_ap = 0 then ""
        else ""
    end as motivo_status
from sources_joined
where
    hierarquia_n1_categoria = "Medicamento"
    and cadastrado_sistema_vitacare_indicador = "sim"
