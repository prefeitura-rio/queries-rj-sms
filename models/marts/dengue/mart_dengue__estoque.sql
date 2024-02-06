{{
    config(
        alias="estoque_cobertura",
        schema="projeto_dengue",
        materialized="table",
    )
}}

with
    estoque as (
        select *
        from {{ ref("mart_estoque__posicao_atual") }}
        where
            id_material in (

                "65050110416",  -- DIPIRONA 500 GOTA
                "65050100372",  -- DIPIRONA 500 COMPRIMIDO
                "65151800400",  -- EQUIPO, SORO MACROGOTAS, INJETOR LATERAL
                "65153501833",  -- GARROTE
                "65151401467",  -- JELCO 18
                "65151401548",  -- JELCO 20
                "65151401629",  -- JELCO 22
                "65050121612",  -- PARACETAMOL 200MG/ML GOTA
                "65050101344",  -- PARACETAMOL 500MG COMPRIMIDO
                "65054100199",  -- SAIS DE RESITRADAÇÃO ORAL
                "65054207155",  -- SORO

                "65150309877",  -- AGULHA HIPODERMICA DESCARTAVEL 25X0.8
                "65150309109",  -- AGULHA HIPODERMICA DE SEGURANCA 25X8 (21G 1")
                -- - "65150307823",  -- AGULHA HIPODERMICA DESCARTAVEL  25X8
                "65150303089",  -- AGULHA HIPODERMICA DESCARTAVEL CANULA ACO INOXIDAVEL, BISEL TRIFACETADO, 25X8

                "66401401130",  -- CAIXA, DESCARTE MATERIAL PERFUROCORTANTE, TAMANHO GRANDE
                "66401401050",  -- CAIXA, DESCARTE MATERIAL PERFUROCORTANTE, TAMANHO PEQUENO
                "65155506700",  -- CAIXA DESCARTAVEL PAPELAO, P/MATERIAL CONTAMINADO 13L
                "65155504414",  -- CAIXA DESCARTAVEL PAPELAO, P/MATERIAL CONTAMINADO 5 A 7L

                "65054207236",  -- GLICOSE 5%, SISTEMA FECHADO, 500ML
                "65054201114",  -- GLICOSE 5% SOLUCAO INJETAVEL ISOTONICA FRASCO 500ML

                "65320000120",  -- LUVA CIRURGICA N.6,5
                "65320000200",  -- LUVA CIRURGICA No 7,0
                "65320000391",  -- LUVA CIRURGICA No 7,5
                "65320000472",  -- LUVA CIRURGICA No 8,0
                "65320000553",  -- LUVA CIRURGICA No 8,5

                "65054207660",  -- RINGER + LACTATO SODICO SISTEMA FECHADO, 500ML
                "65054201548",  -- RINGER + LACTATO SODICO SOLUCAO INJETAVEL FRASCO 500ML

                "65153700879",  -- SCALP 21
                "65153700283",  -- SCALP 21

                "65153700950",  -- SCALP 23
                "65153700364",  -- SCALP 23

                "65153701093",  -- SCALP 25
                "65153700445",  -- SCALP 25

                "65159973141",  -- SERINGA DESC. 5ML C/AG. 25X8
                "65153802108",  -- SERINGA DESCARTAVEL 5ML, C/AGULHA 25X8MM

                "66403110583",  -- TUBO, PLASTICO COM EDTA E GEL  SEPARADOR, PARA COLETA DE SANGUE A VACUO
                "65050024170"  -- TUBO COLETA SANGUE COM EDTA

            -- FALTAM Adaptador vacuo; Luva; Caixa descartável
            )
    ),

    estoque_agrupado as (
        select
            id_cnes,
            id_material,
            estabelecimento_agrupador_sms,
            estabelecimento_area_programatica,
            estabelecimento_tipo_sms,
            estabelecimento_nome_limpo,
            estabelecimento_nome_sigla,
            material_descricao,
            sum(material_quantidade) as material_quantidade,
        from estoque
        group by 1, 2, 3, 4, 5, 6, 7, 8
    ),

    estoque_aps_almoxarifado as (
        select * from {{ ref("int_dengue__estoque_posicao_almoxarifado_padronizado") }}
    ),

    estoque_consolidado as (
        select *
        from estoque_agrupado
        union all
        select *
        from estoque_aps_almoxarifado
    ),

    consumo_30_dias as (
        select *
        from
            {{
                ref(
                    "int_estoque__dispensacao_serie_historica_com_outliers_identificados"
                )
            }}
    ),

    consumo_7_dias as (select * from consumo_30_dias where row_num <= 7),

    cmd_30_dias as (
        select id_cnes, id_material, avg(quantidade_dispensada) as cmd_30_dias
        from consumo_30_dias
        where outlier = "nao"
        group by 1, 2
    ),

    cmd_07_dias as (
        select id_cnes, id_material, avg(quantidade_dispensada) as cmd_07_dias
        from consumo_7_dias
        where outlier = "nao"
        group by 1, 2
    )

select
    e.*,
    c30.cmd_30_dias as material_cmd_30_dias,
    c07.cmd_07_dias as material_cmd_07_dias,
    {{ dbt_utils.safe_divide("e.material_quantidade", "c30.cmd_30_dias") }}
    as material_cobertura_30_dias,
    {{ dbt_utils.safe_divide("e.material_quantidade", "c07.cmd_07_dias") }}
    as material_cobertura_07_dias,
    concat(e.id_material, " - ", e.material_descricao) as material_id_descricao,
    concat(e.material_descricao, " - ", e.id_material) as material_descricao_id
from estoque_consolidado as e
left join
    cmd_30_dias as c30 on e.id_cnes = c30.id_cnes and e.id_material = c30.id_material
left join
    cmd_07_dias as c07 on e.id_cnes = c07.id_cnes and e.id_material = c07.id_material
