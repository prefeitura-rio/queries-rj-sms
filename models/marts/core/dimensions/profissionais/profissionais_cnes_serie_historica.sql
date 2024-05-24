{{
    config(
        schema="saude_dados_mestres",
        alias="profissionais_serie_historica",
        materialized="table",
    )
}}

SELECT
  -- precisa tratar esses campos, limpar espacos em branco, checar se estao com a quantidade certa de caracteres etc
  CONCAT(p.ano,'-',LPAD(CAST(p.mes as STRING),2,'0')) data_registro,
  p.id_estabelecimento_cnes,
  utils.clean_name_string(p.nome) as nome_profissional,
  p.cartao_nacional_saude,
  ocup.descricao as cbo_descricao,
  ocupf.descricao as cbo_fam_descricao,
  vinculacao.DS_VINCULACAO as vinculacao_descricao,
  tipo_vinculo.DS_VINCULO as tipo_vinculo_descricao,
  p.carga_horaria_outros,
  p.carga_horaria_hospitalar,
  p.carga_horaria_ambulatorial,
  p.id_registro_conselho,
  p.tipo_conselho

FROM
  `rj-sms.brutos_cnes_ftp.profissional` as p
LEFT JOIN `rj-sms.brutos_datasus.cbo` as ocup
  ON p.cbo_2002 = ocup.id_cbo
LEFT JOIN `rj-sms.brutos_datasus.cbo_fam` as ocupf
  ON left(p.cbo_2002, 4) = ocupf.id_cbo_familia 

LEFT JOIN (
  SELECT *, CONCAT(CD_VINCULACAO,TP_VINCULO) as cd_tipo_vinculo
  FROM `rj-sms.brutos_cnes_web_staging.tbTpModVinculo`
  WHERE _data_snapshot = (
    SELECT DISTINCT _data_snapshot 
    FROM `rj-sms.brutos_cnes_web_staging.tbTpModVinculo`
    ORDER BY 1 DESC 
    LIMIT 1
  )
  ) as tipo_vinculo
  ON SUBSTRING(p.tipo_vinculo, 1, 4) = tipo_vinculo.cd_tipo_vinculo

LEFT JOIN (
  SELECT *
  FROM `rj-sms.brutos_cnes_web_staging.tbModVinculo`
  WHERE _data_snapshot = (
    SELECT DISTINCT _data_snapshot 
    FROM `rj-sms.brutos_cnes_web_staging.tbModVinculo`
    ORDER BY 1 DESC 
    LIMIT 1
  )
  ) as vinculacao
  ON SUBSTRING(p.tipo_vinculo, 1, 2) = vinculacao.CD_VINCULACAO

WHERE
  ano >= 2008
  AND sigla_uf = "RJ"
  AND (
    indicador_atende_sus = 1
    OR indicador_vinculo_contratado_sus = 1
    OR indicador_vinculo_autonomo_sus = 1
  )
  AND id_estabelecimento_cnes IN (
      SELECT
        DISTINCT id_estabelecimento_cnes
      FROM
        `rj-sms.brutos_cnes_ftp.estabelecimento`
      WHERE
        ano >= 2008
        AND sigla_uf = "RJ"
        AND id_municipio_6 = "330455"
        AND indicador_vinculo_sus = 1
    )