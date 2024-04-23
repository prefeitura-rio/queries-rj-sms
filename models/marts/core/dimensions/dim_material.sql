{{
    config(
        alias="material",
    )
}}

with

    material_mestre as (select * from {{ ref("raw_sheets__material_mestre") }})

select
    -- Primary Key
    id_material,

    -- Common columns
    descricao,
    concentracao,
    forma_farmaceutica,
    apresentacao,
    trim(
        concat(
            descricao,
            ", ",
            coalesce(concentracao, " "),
            " ",
            coalesce(apresentacao, forma_farmaceutica, " ")
        )
    ) as nome,
    hierarquia_n1_categoria,
    hierarquia_n2_subcategoria,
    controlado_indicador,
    controlado_tipo,
    remume_indicador,
    remume_listagem_relacao,
    remume_listagem_basico_indicador,
    remume_listagem_uso_interno_indicador,
    remume_listagem_hospitalar_indicador,
    remume_listagem_antiseptico_indicador,
    remume_listagem_estrategico_indicador,
    remume_disponibilidade_relacao,
    remume_disponibilidade_cms_indicador,
    remume_disponibilidade_cf_indicador,
    remume_disponibilidade_cse_indicador,
    remume_disponibilidade_policlinica_indicador,
    remume_disponibilidade_hospital_indicador,
    remume_disponibilidade_maternidade_indicador,
    remume_disponibilidade_caps_indicador,
    remume_disponibilidade_upa_indicador,
    remume_disponibilidade_cer_indicador,
    remume_disponibilidade_unidades_especificas_lista,
    cadastrado_sistema_vitacare_indicador
from material_mestre
order by
    remume_indicador desc,
    hierarquia_n1_categoria desc,
    descricao asc,
    concentracao asc,
    forma_farmaceutica asc,
    apresentacao asc


-- select
-- -- Primary Key
-- cd_material as id_material,
--
-- -- Foreign Keys
-- mat.cd_grupo as id_grupo,
-- mat.cd_classe as id_classe,
-- mat.cd_subclasse as id_subclasse,
--
-- -- Common columns
-- grupo.ds_grupo as hierarquia_n1_grupo,
-- classe.ds_classe as hierarquia_n2_classe,
-- subclasse.ds_subclasse as hierarquia_n3_subclasse,
-- if(mat.cd_grupo_classe = "6505", "Medicamento", "Insumo") as natureza,
-- concat(mat.nm_padronizado, " ", mat.nm_complementar_material) as nome,
-- mat.ds_detalhe_material as nome_complementar,
-- mat.unidade,
-- mat.st_status as status,
-- mat.remume
--
-- from material_distinct as mat
-- left join grupo using (cd_grupo)
-- left join classe using (cd_grupo_classe)
-- left join subclasse using (cd_grupo_classe_subclasse)