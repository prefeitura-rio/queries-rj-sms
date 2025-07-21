
{{
    config(
        enabled=true,
        alias="categorias_risco_desconcatenadas",
    )
}}

WITH riscos_separados AS (
  SELECT 
    id_gestacao,
    TRIM(risco) AS categoria_risco
 FROM {{ ref('mart_bi_gestacoes__linha_tempo') }},
    UNNEST(SPLIT(categorias_risco, ';')) AS risco
  WHERE 
    TRIM(risco) != ''  -- Remove entradas vazias
)

SELECT 
  id_gestacao,
  categoria_risco
FROM 
  riscos_separados
WHERE 
  categoria_risco IS NOT NULL

