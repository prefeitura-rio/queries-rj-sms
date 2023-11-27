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
        codigo = "nan", null, regexp_replace(codigo, r'[^0-9]', '')
    ) as id_material,

    -- Foreign keys

    -- Common fields
    if(grupo = "nan", null, grupo) as remume_grupo,
    if(
        denominacao_generica = "nan", null, denominacao_generica
    ) as material_descricao_generica,
    if(concentracao = "nan", null, concentracao) as material_concetracao,
    if(
        forma_farmaceutica = "nan", null, forma_farmaceutica
    ) as material_forma_farmaceutica,
    if(apresentacao = "nan", null, apresentacao) as material_apresentacao,
    if(disponibilidade = "nan", null, disponibilidade) as estabelecimento_disponibilidade_string,
    SPLIT(SUBSTR(disponibilidade, 1, LENGTH(disponibilidade) - 1), ';') as estabelecimento_disponibilidade,

from source

