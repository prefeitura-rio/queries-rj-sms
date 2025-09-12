{{
    config(
        alias="cnpj",
        materialized="table",
        tags=["daily"]
    )
}}

SELECT
  cnpj,
  razao_social,
  nome_fantasia,
  cnae_fiscal,
  cnae_secundarias,
  responsavel.cpf,
  inicio_atividade_data,
  situacao_cadastral,
  endereco
FROM {{ source("brutos_bcadastro", "cnpj") }}
WHERE endereco.municipio_nome = 'Rio de Janeiro'