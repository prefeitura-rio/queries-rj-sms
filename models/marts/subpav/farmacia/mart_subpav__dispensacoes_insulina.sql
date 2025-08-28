{{
    config(
        enabled=true,
        alias="dispensacoes_insulina",
    )
}}

WITH 
  ultimos_3_meses_completos AS (
    SELECT 
      DATE_TRUNC(DATE_SUB(CURRENT_DATE(), INTERVAL 3 MONTH), MONTH) AS data_inicio,
      DATE_TRUNC(CURRENT_DATE(), MONTH) AS data_fim
  ),

  datapoints AS (
    SELECT *
    FROM {{ ref('raw_prontuario_vitacare_api_centralizadora__estoque_movimento') }}, ultimos_3_meses_completos
    WHERE 
      -- Insulina
      id_material IN ('65051101631','65051101712') AND

      -- Filtro de data
      data_particao >= ultimos_3_meses_completos.data_inicio AND 
      data_particao < ultimos_3_meses_completos.data_fim AND

      -- Filtro de dispensacao
      dispensacao_paciente_cpf IS NOT null AND

      -- Filtro de tipo de dispensacao
      estoque_movimento_tipo IN (
        'DISPENSA DE MEDICAMENTOS COM PRESCRIÇÃO',
        'DISPENSAÇÃO DE RECEITA EXTERNA',
        'DISPENSAÇÃO DE RECEITA EXTERNA COM DATA ANTERIOR'
      )
  ),

  agrupado AS (
    SELECT 
      estabelecimento_nome,
      area_programatica as estabelecimento_ap,
      id_cnes as estabelecimento_cnes,
      data_particao as data_referencia, 
      count(*) as quant_dispensacoes,
      array_agg(
        struct(
          id_material,
          estoque_movimento_tipo,
          dispensacao_paciente_cpf
        ) 
      )as dispensacoes
    from datapoints
    group by estabelecimento_nome, estabelecimento_ap, estabelecimento_cnes, data_particao
    order by data_particao desc
  )


SELECT *
from agrupado