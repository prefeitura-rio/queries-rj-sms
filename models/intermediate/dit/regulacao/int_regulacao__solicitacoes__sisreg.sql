{{
  config(
    enabled = false,
    schema = "intermediario_regulacao",
    alias  = "solicitacoes_sisreg",
    partition_by = {
      "field": "data_particao",
      "data_type": "date",
      "granularity": "month"
    },
  )
}}
