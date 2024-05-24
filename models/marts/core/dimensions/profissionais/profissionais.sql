{{
    config(
        schema="saude_dados_mestres",
        alias="profissionais",
        materialized="table",
    )
}}


SELECT 
id_estabelecimento_cnes,
nome_profissional,
cartao_nacional_saude,
id_registro_conselho,
tipo_conselho,
cbo_fam_descricao,
ARRAY_AGG(DISTINCT cbo_descricao IGNORE NULLS) AS lista_especialidades_prestadas
FROM {{ ref("profissionais_cnes_serie_historica") }}
GROUP BY 1,2,3,4,5,6