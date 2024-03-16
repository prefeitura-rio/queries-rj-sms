{{
    config(
        schema="brutos_sheets",
        alias="material_remume",
    )
}}


with source as (select * from {{ source("brutos_sheets_staging", "material_remume") }})


select
    -- Primary key
    if(
        codigo_limpo = "nan", null, regexp_replace(codigo_limpo, r'[^0-9]', '')
    ) as id_material,

    -- Common fields
    if(
        denominacao_generica = "nan", null, denominacao_generica
    ) as material_descricao_generica,
    if(concentracao = "nan", null, concentracao) as material_concetracao,
    if(
        forma_farmaceutica = "nan", null, forma_farmaceutica
    ) as material_forma_farmaceutica,
    if(apresentacao = "nan", null, apresentacao) as material_apresentacao,
    -- if(disponibilidade = "nan", null, disponibilidade) as estabelecimento_disponibilidade_string,
    SPLIT(SUBSTR(disponibilidade, 1, LENGTH(disponibilidade) - 1), ';') as estabelecimento_disponibilidade,
    SPLIT(SUBSTR(grupos, 1, LENGTH(grupos) ), ';') as remume_grupos,
    if(remume_basico = "nan", null, remume_basico) as remume_basico,
    if(remume_uso_interno = "nan", null, remume_uso_interno) as remume_uso_interno,
    if(remume_hospitalar = "nan", null, remume_hospitalar) as remume_hospitalar,
    if(remume_antiseptico = "nan", null, remume_antiseptico) as remume_antiseptico,
    if(remume_estrategico = "nan", null, remume_estrategico) as remume_estrategico,

from source

