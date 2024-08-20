{{
    config(
        alias="material",
        tags=["daily"]
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
    ativo_indicador,
    controlado_indicador,
    controlado_tipo,
    consumo_minimo,
    abastecimento_responsavel,
    abastecimento_frequencia,
    classificacao_xyz,
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
    farmacia_popular_disponibilidade_indicador,
    cadastrado_sistema_vitacare_indicador
from material_mestre
order by
    remume_indicador desc,
    hierarquia_n1_categoria desc,
    descricao asc,
    concentracao asc,
    forma_farmaceutica asc,
    apresentacao asc
