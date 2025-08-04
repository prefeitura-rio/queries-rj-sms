{{
    config(
        enabled=true,
        alias="atendimentos_prenatal_aps",
    )
}}

WITH 

marcadores_temporais AS (
 SELECT
   id_gestacao,
   id_paciente,
   cpf,
   nome,
   numero_gestacao,
   idade_gestante,
   data_inicio,
   data_fim,
   data_fim_efetiva,
   fase_atual
 FROM {{ ref('mart_bi_gestacoes__gestacoes') }}
),


-- Peso dentro de -180 a +84 dias da data_inicio
peso_filtrado AS (
 SELECT
   mt.id_gestacao,
   mt.id_paciente,
   ea.entrada_data,
   ea.medidas.peso,
   DATE_DIFF(ea.entrada_data, mt.data_inicio, DAY) AS dias_diferenca
 FROM {{ ref('mart_historico_clinico__episodio') }} ea
 JOIN marcadores_temporais mt
   ON ea.paciente.id_paciente = mt.id_paciente
 WHERE ea.medidas.peso IS NOT NULL
   AND ea.entrada_data BETWEEN DATE_SUB(mt.data_inicio, INTERVAL 180 DAY)
                           AND DATE_ADD(mt.data_inicio, INTERVAL 84 DAY)
),


peso_proximo_inicio AS (
 SELECT *
 FROM (
   SELECT *,
          ROW_NUMBER() OVER (
            PARTITION BY id_gestacao
            ORDER BY ABS(dias_diferenca)
          ) AS rn
   FROM peso_filtrado
 )
 WHERE rn = 1
),


-- Altura moda preferencial entre 1 ano antes e fim da gestação
alturas_filtradas AS (
 SELECT
   mt.id_gestacao,
   ea.paciente.id_paciente,
   ea.medidas.altura,
   DATE_DIFF(mt.data_inicio, ea.entrada_data, DAY) AS dias_antes_inicio,
   DATE_DIFF(ea.entrada_data, COALESCE(mt.data_fim_efetiva, CURRENT_DATE()), DAY) AS dias_apos_inicio
 FROM {{ ref('mart_historico_clinico__episodio') }} ea
 JOIN marcadores_temporais mt
   ON ea.paciente.id_paciente = mt.id_paciente
 WHERE ea.medidas.altura IS NOT NULL
),


altura_preferencial AS (
 SELECT
   id_gestacao,
   id_paciente,
   CAST(altura AS FLOAT64) AS altura_cm,
   COUNT(*) AS freq,
   ROW_NUMBER() OVER (
     PARTITION BY id_gestacao
     ORDER BY COUNT(*) DESC
   ) AS rn
 FROM alturas_filtradas
 WHERE dias_antes_inicio <= 365 AND dias_apos_inicio <= 0
 GROUP BY id_gestacao, id_paciente, altura
),


altura_fallback AS (
 SELECT
   id_gestacao,
   id_paciente,
   CAST(altura AS FLOAT64) AS altura_cm,
   COUNT(*) AS freq,
   ROW_NUMBER() OVER (
     PARTITION BY id_gestacao
     ORDER BY COUNT(*) DESC
   ) AS rn
 FROM alturas_filtradas
 GROUP BY id_gestacao, id_paciente, altura
),


altura_moda_completa AS (
 SELECT * FROM altura_preferencial WHERE rn = 1
 UNION ALL
 SELECT * FROM altura_fallback
 WHERE id_gestacao NOT IN (SELECT id_gestacao FROM altura_preferencial WHERE rn = 1)
),


-- Junta peso + altura
peso_altura_inicio AS (
 SELECT
   p.id_gestacao,
   p.id_paciente,
   p.peso,
   a.altura_cm / 100 AS altura_m,
   ROUND(p.peso / POW(a.altura_cm / 100, 2), 1) AS imc_inicio,
   CASE
     WHEN ROUND(p.peso / POW(a.altura_cm / 100, 2), 1) < 18 THEN 'Baixo peso'
     WHEN ROUND(p.peso / POW(a.altura_cm / 100, 2), 1) < 25 THEN 'Eutrófico'
     WHEN ROUND(p.peso / POW(a.altura_cm / 100, 2), 1) < 30 THEN 'Sobrepeso'
     ELSE 'Obesidade'
   END AS classificacao_imc_inicio
 FROM peso_proximo_inicio p
 JOIN altura_moda_completa a ON p.id_gestacao = a.id_gestacao
),


-- Atendimentos de pré-natal APS
atendimentos_filtrados AS (
 SELECT
   ea.id_hci,
   ea.paciente.id_paciente,
   ea.entrada_data,
   ea.estabelecimento.nome AS estabelecimento,
   ea.estabelecimento.estabelecimento_tipo,
   ea.profissional_saude_responsavel.nome AS profissional_nome,
   ea.profissional_saude_responsavel.especialidade AS profissional_categoria,
   ea.medidas.altura,
   ea.medidas.peso,
   ea.medidas.imc,
   ea.medidas.pressao_sistolica,
   ea.medidas.pressao_diastolica,
   ea.motivo_atendimento,
   ea.desfecho_atendimento,
   c.id AS cid,
   -- STRING_AGG(DISTINCT c.id, '; ' ORDER BY c.id) AS cid_string
 FROM {{ ref('mart_historico_clinico__episodio') }} ea
 --Ajuste UNNEST (foi retirado a vírgula ao fim da linha acima)
    LEFT JOIN UNNEST(ea.condicoes) AS c
 WHERE ea.subtipo = 'Atendimento SOAP'
   AND LOWER(ea.prontuario.fornecedor) = 'vitacare'
  --  AND c.situacao = 'ATIVO'
  --  AND (c.id = 'Z321' OR c.id LIKE 'Z34%' OR c.id LIKE 'Z35%')
   AND ea.profissional_saude_responsavel.especialidade IN (
     'Médico da estratégia de saúde da família',
     'Enfermeiro da estratégia saúde da família',
     'Enfermeiro - Modelo B',
     'Médico Clínico',
     'Médico Ginecologista e Obstetra - NASF',
     'Médico Ginecologista - Modelo B',
     'Médico Clinico - Modelo B',
     'Enfermeiro obstétrico',
     'Enfermeiro',
     'Enfermeiro Obstetrico - Nasf',
     'Médico Generalista',
     'Médico de Família e Comunidade'
   )
),


-- Join com gestação
atendimentos_gestacao AS (
 SELECT
   af.*,
   mt.id_gestacao,
   mt.data_inicio,
   mt.data_fim_efetiva,
   mt.fase_atual,
   DATE_DIFF(af.entrada_data, mt.data_inicio, WEEK) AS ig_consulta,
   CASE
     WHEN DATE_DIFF(af.entrada_data, mt.data_inicio, WEEK) <= 13 THEN 1
     WHEN DATE_DIFF(af.entrada_data, mt.data_inicio, WEEK) <= 27 THEN 2
     ELSE 3
   END AS trimestre_consulta
 FROM atendimentos_filtrados af
 JOIN marcadores_temporais mt
   ON af.id_paciente = mt.id_paciente
  AND af.entrada_data BETWEEN mt.data_inicio AND COALESCE(mt.data_fim_efetiva, CURRENT_DATE())
),


-- Prescrições
prescricoes_aggregadas AS (
 SELECT
   ea.id_hci,
   STRING_AGG(p.nome, ', ') AS prescricoes
 FROM {{ ref('mart_historico_clinico__episodio') }} ea
  --Ajuste UNNEST (foi retirado a vírgula ao fim da linha acima)
  LEFT JOIN UNNEST(ea.prescricoes) AS p
 WHERE ea.subtipo = 'Atendimento SOAP'
   AND LOWER(ea.prontuario.fornecedor) = 'vitacare'
 GROUP BY ea.id_hci
),


-- Final com cálculos
consultas_enriquecidas AS (
 SELECT
   ag.*,
   presc.prescricoes,
   ROW_NUMBER() OVER (PARTITION BY ag.id_gestacao ORDER BY ag.entrada_data) AS numero_consulta,
  
   pai.peso AS peso_inicio,
   pai.altura_m,
   pai.imc_inicio,
   pai.classificacao_imc_inicio,


   ag.peso - pai.peso AS ganho_peso_acumulado,
   ROUND(ag.peso / POW(pai.altura_m, 2), 1) AS imc_consulta


 FROM atendimentos_gestacao ag
 LEFT JOIN prescricoes_aggregadas presc ON ag.id_hci = presc.id_hci
 LEFT JOIN peso_altura_inicio pai ON ag.id_gestacao = pai.id_gestacao
)


-- Resultado final
SELECT
 id_gestacao,
 id_paciente,
 entrada_data AS data_consulta,
 numero_consulta,
 ig_consulta,
 trimestre_consulta,
 fase_atual,


 peso_inicio,
 altura_m AS altura_inicio,
 imc_inicio,
 classificacao_imc_inicio,


 peso,
 imc_consulta,
 ganho_peso_acumulado,


 pressao_sistolica,
 pressao_diastolica,


 motivo_atendimento AS descricao_s,
 cid,
 desfecho_atendimento AS desfecho,
 prescricoes,


 estabelecimento,
 profissional_nome,
 profissional_categoria


FROM consultas_enriquecidas
ORDER BY
 data_consulta DESC


