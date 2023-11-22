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
    if(disponibilidade = "nan", null, disponibilidade) as estabelecimento_disponibilidade,
    if(disponivel_cms = "nan", null, disponivel_cms) as estabelecimento_disponivel_cms,
    if(disponivel_cf = "nan", null, disponivel_cf) as estabelecimento_disponivel_cf,
    if(disponivel_policlinica = "nan", null, disponivel_policlinica) as estabelecimento_disponivel_policlinica,
    if(disponivel_hospital = "nan", null, disponivel_hospital) as estabelecimento_disponivel_hospital,
    if(disponivel_maternidade = "nan", null, disponivel_maternidade) as estabelecimento_disponivel_maternidade,
    if(disponivel_caps = "nan", null, disponivel_caps) as estabelecimento_disponivel_caps,
    if(disponivel_upa = "nan", null, disponivel_upa) as estabelecimento_disponivel_upa,
    if(disponivel_cer = "nan", null, disponivel_cer) as estabelecimento_disponivel_cer,
    if(disponivel_unidades_especificas = "nan", null, disponivel_unidades_especificas) as estabelecimento_disponivel_unidades_especificas,


from source

