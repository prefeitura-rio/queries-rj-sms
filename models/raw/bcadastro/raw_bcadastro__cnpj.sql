{{
    config(
        alias="cnpj",
        materialized="table",
        cluster_by="situacao",
        partition_by={
          "field": "cnpj_particao",
          "data_type": "int64",
          "range": {"start": 0, "end": 99999999999, "interval": 2499999999975},
          },
        tags=["monthly"]
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
  situacao_cadastral.descricao as situacao,
  endereco,
  cast(cnpj.cnpj as int64) as cnpj_particao
FROM {{ source("brutos_bcadastro", "cnpj") }}