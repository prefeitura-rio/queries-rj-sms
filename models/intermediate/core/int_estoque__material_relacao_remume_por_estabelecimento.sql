with
    -- Sources 
    remume as (
        select
            *
        from {{ ref("raw_sheets__material_mestre") }}
        where remume_indicador = "sim" and ativo_indicador = "sim"
    ),

    estabelecimento as (select * from {{ ref("dim_estabelecimento") }}),

    -- Relação de materiais da REMUME por Unidade
    -- - Unidades de Saúde
    combinacao_estabelecimento_remume as (
        select
            est.id_cnes,
            est.tipo_sms_simplificado,
            est.tipo_sms_agrupado,
            est.prontuario_versao,
            est.prontuario_estoque_tem_dado,
            remume.cadastrado_sistema_vitacare_indicador,
            remume.remume_disponibilidade_relacao,
            remume.id_material,
            remume.remume_listagem_relacao,
            remume.remume_listagem_basico_indicador,
            remume.remume_listagem_hospitalar_indicador,
            remume.remume_listagem_uso_interno_indicador,
            remume.remume_listagem_estrategico_indicador,
        from estabelecimento as est
        cross join remume
    ),

    relacao_remume_unidades as (
        select *
        from combinacao_estabelecimento_remume
        where
            tipo_sms_simplificado in unnest(remume_disponibilidade_relacao)
            or id_cnes in unnest(remume_disponibilidade_relacao)
        order by id_cnes
    ),

    relacao_remume_unidades_com_dados as (
        select *
        from relacao_remume_unidades
        where
            (
                tipo_sms_agrupado = "APS"
                and prontuario_versao = "vitacare"
                and cadastrado_sistema_vitacare_indicador = "sim"
            )  -- somente mostrar itens cadastrados na vitacare para APS
            or tipo_sms_agrupado <> "APS"  -- # TODO: revisar para as demais unidades
    ),

    -- - TPC
    remume_distintos as (
        select
            id_material,
            remume_listagem_relacao,
            remume_listagem_basico_indicador,
            remume_listagem_hospitalar_indicador,
            remume_listagem_uso_interno_indicador,
            remume_listagem_estrategico_indicador,
        from remume
        where
            array_length(remume_disponibilidade_relacao) > 2
            or (
                array_length(remume_disponibilidade_relacao) = 1
                and not (
                    'HOSPITAL' in unnest(remume_disponibilidade_relacao)
                    or 'MATERNIDADE' in unnest(remume_disponibilidade_relacao)
                )
            )
            or (
                array_length(remume_disponibilidade_relacao) = 2
                and not (
                    'HOSPITAL' in unnest(remume_disponibilidade_relacao)
                    and 'MATERNIDADE' in unnest(remume_disponibilidade_relacao)
                )
            )  -- TPC só abastece APS, então só considerar itens que não são exclusivos para hospitais e maternidades
    ),

    relacao_remume_tpc as (
        select
            "tpc" as id_cnes,
            "TPC" as tipo_sms_simplificado,
            "TPC" as tipo_sms_agrupado,
            "" as prontuario_versao,
            "" as prontuario_estoque_tem_dado,
            "" as cadastrado_sistema_vitacare_indicador,
            array["TPC"] as remume_disponibilidade_relacao,
            remume.id_material,
            remume.remume_listagem_relacao,
            remume.remume_listagem_basico_indicador,
            remume.remume_listagem_hospitalar_indicador,
            remume.remume_listagem_uso_interno_indicador,
            remume.remume_listagem_estrategico_indicador
        from remume_distintos as remume
    )

-- Result
select *
from relacao_remume_unidades_com_dados
union all
select *
from relacao_remume_tpc
