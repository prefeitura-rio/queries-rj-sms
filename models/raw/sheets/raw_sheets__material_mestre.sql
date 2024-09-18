{{
    config(
        schema="brutos_sheets",
        alias="material_mestre",
    )
}}

with
    source as (select * from {{ source("brutos_sheets_staging", "material_mestre") }}),
    casted as (
        select
            {{clean_numeric_string("codigo")}} as id_material,
            denominacao_generica as descricao,
            concentracao,
            forma_farmaceutica,
            apresentacao,
            categoria as hierarquia_n1_categoria,
            subcategoria as hierarquia_n2_subcategoria,
            ativo_indicador,
            controlado_indicador,
            controlado_tipo,
            coalesce(safe_cast(consumo_minimo as int64), 1) as consumo_minimo,
            abastecimento_responsavel,
            abastecimento_frequencia,
            classificacao_xyz,
            remume_indicador,
            remume_listagens as remume_listagem_relacao,
            -- SPLIT(SUBSTR(remume_listagens, 1, LENGTH(remume_listagens) - 1), ';')
            -- as remume_listagens,
            ifnull(
                remume_listagem_basico_indicador, "nao"
            ) as remume_listagem_basico_indicador,
            ifnull(
                remume_listagem_uso_interno_indicador, "nao"
            ) as remume_listagem_uso_interno_indicador,
            ifnull(
                remume_listagem_hospitalar_indicador, "nao"
            ) as remume_listagem_hospitalar_indicador,
            ifnull(
                remume_listagem_antiseptico_indicador, "nao"
            ) as remume_listagem_antiseptico_indicador,
            ifnull(
                remume_listagem_estrategico_indicador, "nao"
            ) as remume_listagem_estrategico_indicador,
            remume_disponibilidades as remume_disponibilidade_relacao,
            -- SPLIT(SUBSTR(remume_disponibilidades, 1,
            -- LENGTH(remume_disponibilidades) - 1), ';') as
            -- remume_disponibilidade_relacao,
            ifnull(
                remume_disponibilidade_cms_indicador, "nao"
            ) as remume_disponibilidade_cms_indicador,
            ifnull(
                remume_disponibilidade_cf_indicador, "nao"
            ) as remume_disponibilidade_cf_indicador,
            ifnull(
                remume_disponibilidade_cse_indicador, "nao"
            ) as remume_disponibilidade_cse_indicador,
            ifnull(
                remume_disponibilidade_policlinica_indicador, "nao"
            ) as remume_disponibilidade_policlinica_indicador,
            ifnull(
                remume_disponibilidade_hospital_indicador, "nao"
            ) as remume_disponibilidade_hospital_indicador,
            ifnull(
                remume_disponibilidade_maternidade_indicador, "nao"
            ) as remume_disponibilidade_maternidade_indicador,
            ifnull(
                remume_disponibilidade_caps_indicador, "nao"
            ) as remume_disponibilidade_caps_indicador,
            ifnull(
                remume_disponibilidade_upa_indicador, "nao"
            ) as remume_disponibilidade_upa_indicador,
            ifnull(
                remume_disponibilidade_cer_indicador, "nao"
            ) as remume_disponibilidade_cer_indicador,
            remume_disponibilidade_unidades_especificas_lista,
            coalesce(disponibilidade_farmacia_popular_indicador, "nao") as farmacia_popular_disponibilidade_indicador,
            if(
                contains_substr(cadastrado_sistema_vitacare_indicador, "nao"),
                "nao",
                "sim"
            ) as cadastrado_sistema_vitacare_indicador,

        from source
    )
select *
from casted