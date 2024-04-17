{{
    config(
        schema="brutos_sheets",
        alias="material_mestre",
    )
}}

with source as (
      select * from {{ source('brutos_sheets_staging', 'material_mestre') }}
),
casted as (
    select
        codigo as id_material,
        denominacao_generica as descricao,
        concentracao,
        forma_farmaceutica,
        apresentacao,
        categoria,
        subcategoria,
        controlado_indicador,
        controlado_tipo,
        remume_indicador,
        remume_listagens,
        ifnull(remume_listagem_basico_indicador , "nao") as remume_listagem_basico_indicador,
        ifnull(remume_listagem_uso_interno_indicador, "nao") as remume_listagem_uso_interno_indicador,
        ifnull(remume_listagem_hospitalar_indicador, "nao") as remume_listagem_hospitalar_indicador,
        ifnull(remume_listagem_antiseptico_indicador, "nao") as remume_listagem_antiseptico_indicador,
        ifnull(remume_listagem_estrategico_indicador, "nao") as remume_listagem_estrategico_indicador,
        remume_disponibilidades,
        ifnull(remume_disponibilidade_cms_indicador, "nao") as remume_disponibilidade_cms_indicador,
        ifnull(remume_disponibilidade_cf_indicador, "nao") as remume_disponibilidade_cf_indicador,
        ifnull(remume_disponibilidade_cse_indicador, "nao") as remume_disponibilidade_cse_indicador,
        ifnull(remume_disponibilidade_policlinica_indicador, "nao") as remume_disponibilidade_policlinica_indicador,
        ifnull(remume_disponibilidade_hospital_indicador, "nao") as remume_disponibilidade_hospital_indicador,
        ifnull(remume_disponibilidade_maternidade_indicador, "nao") as remume_disponibilidade_maternidade_indicador,
        ifnull(remume_disponibilidade_caps_indicador, "nao") as remume_disponibilidade_caps_indicador,
        ifnull(remume_disponibilidade_upa_indicador, "nao") as remume_disponibilidade_upa_indicador,
        ifnull(remume_disponibilidade_cer_indicador, "nao") as remume_disponibilidade_cer_indicador,
        remume_disponibilidade_unidades_especificas_lista,
        cadastrado_sistema_vitacare_indicador,

    from source
)
select * from casted
  