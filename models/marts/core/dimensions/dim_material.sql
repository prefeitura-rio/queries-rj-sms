{{
    config(
        alias="material",
    )
}}

with
    material as (
        select
            concat(cd_grupo, cd_classe) as cd_grupo_classe,
            concat(cd_grupo, cd_classe, cd_subclasse) as cd_grupo_classe_subclasse,
            *
        from {{ source("sigma", "material") }}
    ),
    grupo as (select cd_grupo, ds_grupo from {{ source("sigma", "grupo") }}),
    classe as (
        select concat(cd_grupo, cd_classe) as cd_grupo_classe, ds_classe
        from {{ source("sigma", "classe") }}
    ),
    subclasse as (
        select
            concat(cd_grupo, cd_classe, cd_subclasse) as cd_grupo_classe_subclasse,
            ds_subclasse
        from {{ source("sigma", "subclasse") }}
    )

select
    -- Primary Key
    cd_material as id_material,

    -- Foreign Keys
    mat.cd_grupo as id_grupo,
    mat.cd_classe as id_classe,
    mat.cd_subclasse as id_subclasse,

    -- Common columns
    grupo.ds_grupo as hierarquia_n1_grupo,
    classe.ds_classe as hierarquia_n2_classe,
    subclasse.ds_subclasse as hierarquia_n3_subclasse,
    if(mat.cd_grupo_classe = "6505", "Medicamento", "Insumo") as natureza,
    concat(mat.nm_padronizado, " ", mat.nm_complementar_material) as nome,
    mat.ds_detalhe_material as nome_complementar,
    mat.unidade,
    mat.st_status as status,
    mat.remume

from material as mat
left join grupo using (cd_grupo)
left join classe using (cd_grupo_classe)
left join subclasse using (cd_grupo_classe_subclasse)
