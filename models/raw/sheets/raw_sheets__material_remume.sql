{{
    config(
        schema="brutos_sheets",
        alias="material_remume",
    )
}}


with source as (select * from {{ source("brutos_sheets_staging", "material_remume") }})


select
    if(
        codigo = "nan", null, regexp_replace(codigo, r'[^a-zA-Z0-9]', '')
    ) as id_material,
    if(
        denominacao_generica = "nan", null, denominacao_generica
    ) as material_descricao_generica,
    if(concentracao = "nan", null, concentracao) as material_concetracao,
    if(
        forma_farmaceutica = "nan", null, forma_farmaceutica
    ) as material_forma_farmaceutica,
    if(apresentacao = "nan", null, apresentacao) as material_apresentacao,
    if(local_acesso = "nan", null, local_acesso) as estabelecimento_local_acesso,
    if(programa = "nan", null, programa) as programa,
    if(observacao = "nan", null, observacao) as observacao,
    if(grupo = "nan", null, grupo) as remume_grupo,

from source
