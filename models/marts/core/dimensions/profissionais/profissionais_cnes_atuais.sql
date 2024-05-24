{{
    config(
        schema="saude_dados_mestres",
        alias="profissionais_atuais",
        materialized="table",
    )
}}


SELECT 
  id_estabelecimento_cnes,
  nome_profissional,
  cartao_nacional_saude,
  cbo_descricao,
  cbo_fam_descricao,
  vinculacao_descricao,
  tipo_vinculo_descricao,
  carga_horaria_outros,
  carga_horaria_hospitalar,
  carga_horaria_ambulatorial,
  id_registro_conselho,
  tipo_conselho,
  data_registro as data_ultima_atualizacao

FROM {{ ref("profissionais_cnes_serie_historica") }}
WHERE data_registro = (
    SELECT DISTINCT data_registro 
    FROM {{ ref("profissionais_cnes_serie_historica") }}
    ORDER BY 1 DESC 
    LIMIT 1
)