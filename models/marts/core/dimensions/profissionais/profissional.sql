{{
    config(
        schema="saude_dados_mestres",
        alias="profissional",
        materialized="table",
    )
}}
WITH dados_c_cns as (
SELECT 
  base_mestre.nome_profissional,
  base_mestre.cod_profissional_sus,
  base_mestre.cartao_nacional_saude,
  base_mestre.cbo_fam_descricao,
  base_mestre.id_registro_conselho,
  base_mestre.tipo_conselho,
FROM
(
    SELECT *,
        ROW_NUMBER() OVER (PARTITION BY nome_profissional,cod_profissional_sus ORDER BY data_registro DESC) as ordernacao
    FROM {{ ref("alocacao_cnes_serie_historica") }}
) as base_mestre
WHERE cartao_nacional_saude is not null
AND ordernacao = 1
),

dados_s_cns as(
SELECT 
  base_mestre.nome_profissional,
  base_mestre.cod_profissional_sus,
  base_mestre.cartao_nacional_saude,
  base_mestre.cbo_fam_descricao,
  base_mestre.id_registro_conselho,
  base_mestre.tipo_conselho,
FROM
(
    SELECT *,
        ROW_NUMBER() OVER (PARTITION BY nome_profissional,cod_profissional_sus ORDER BY data_registro DESC) as ordernacao
    FROM {{ ref("alocacao_cnes_serie_historica") }}
) as base_mestre
WHERE cartao_nacional_saude is null
AND ordernacao = 1
)

SELECT base_cpf.cpf,dados_c_cns.*
FROM dados_c_cns
LEFT JOIN `rj-sms-dev.brutos_plataforma_smsrio.profissional_saude_cpf` as base_cpf
ON cartao_nacional_saude = base_cpf.cns_procurado
UNION ALL (SELECT NULL as cpf,* FROM dados_s_cns)
