with
    -- Sources 
    remume as (
        select
            *,
            split(
                substr(
                    remume_disponibilidade_relacao,
                    1,
                    length(remume_disponibilidade_relacao) - 1
                ),
                ';'
            ) as remume_disponibilidade_relacao_array,
        from {{ ref("raw_sheets__material_mestre") }}
        where remume_indicador = "sim"
    ),

    estabelecimento as (select * from {{ ref("dim_estabelecimento") }}),

    -- Relação de materiais da REMUME por Unidade
    -- - Unidades de Saúde
    combinacao_estabelecimento_remume as (
        select
            est.id_cnes,
            est.tipo_sms_simplificado,
            est.agrupador_sms,
            est.prontuario_versao,
            est.prontuario_estoque_tem_dado,
            remume.cadastrado_sistema_vitacare_indicador,
            remume.remume_disponibilidade_relacao_array,
            remume.id_material,
            remume.remume_listagem_relacao,
            remume.remume_listagem_basico_indicador,
            remume.remume_listagem_hospitalar_indicador,
            remume.remume_listagem_uso_interno_indicador,
            remume.remume_listagem_antiseptico_indicador,
            remume.remume_listagem_estrategico_indicador
        from estabelecimento as est
        cross join remume
    ),

    relacao_remume_unidades as (
        select *
        from combinacao_estabelecimento_remume
        where
            tipo_sms_simplificado in unnest(remume_disponibilidade_relacao_array)
            or id_cnes in unnest(remume_disponibilidade_relacao_array)
        order by id_cnes
    ),

    relacao_remume_unidades_com_dados as (
        select *
        from relacao_remume_unidades
        where
            (
                agrupador_sms = "APS"
                and prontuario_versao = "vitacare"
                and cadastrado_sistema_vitacare_indicador = "sim"
            )  -- somente mostrar itens cadastrados na vitacare para APS
            or agrupador_sms <> "APS"  -- # TODO: revisar para as demais unidades
    ),

    -- - TPC
    remume_distintos as (
        select
            id_material,
            remume_disponibilidade_relacao_array,
            remume_listagem_relacao,
            remume_listagem_basico_indicador,
            remume_listagem_hospitalar_indicador,
            remume_listagem_uso_interno_indicador,
            remume_listagem_antiseptico_indicador,
            remume_listagem_estrategico_indicador,
        from remume
        where
            array_length(remume_disponibilidade_relacao_array) > 2
            or (
                array_length(remume_disponibilidade_relacao_array) = 1
                and not (
                    contains_substr(
                        array_to_string(remume_disponibilidade_relacao_array, ','),
                        "HOSPITAL"
                    )
                    or (
                        contains_substr(
                            array_to_string(remume_disponibilidade_relacao_array, ','),
                            "MATERNIDADE"
                        )
                    )
                )
            )
            or (
                array_length(remume_disponibilidade_relacao_array) = 2
                and not (
                    contains_substr(
                        array_to_string(remume_disponibilidade_relacao_array, ','),
                        "HOSPITAL"
                    )
                    and (
                        contains_substr(
                            array_to_string(remume_disponibilidade_relacao_array, ','),
                            "MATERNIDADE"
                        )
                    )
                )
            )  -- TPC só abastece APS, então só considerar itens que não são exclusivos para hospitais e maternidades

    ),

    relacao_remume_tpc as (
        select
            "tpc" as id_cnes,
            "TPC" as tipo_sms_simplificado,
            "TPC" as agrupador_sms,
            "" as prontuario_versao,
            "" as prontuario_estoque_tem_dado,
            "" as cadastrado_sistema_vitacare_indicador,
            array["TPC"] as remume_disponibilidade_relacao_array,
            remume.id_material,
            remume.remume_listagem_relacao,
            remume.remume_listagem_basico_indicador,
            remume.remume_listagem_hospitalar_indicador,
            remume.remume_listagem_uso_interno_indicador,
            remume.remume_listagem_antiseptico_indicador,
            remume.remume_listagem_estrategico_indicador
        from remume_distintos as remume
    )

-- Result
select *
from relacao_remume_unidades_com_dados
union all
select *
from relacao_remume_tpc
