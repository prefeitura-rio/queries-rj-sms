{{
    config(
        schema="intermediario_vacinacao",
        alias="sipni_historico", 
        materialized="table",
        unique_key = ['id_vacinacao'],
        cluster_by= ['id_cnes', 'vacina_nome'],
        partition_by={
            "field": "data_particao",
            "data_type": "date",
            "granularity": "day"
        }
    )
}}

with 
  vacinacao as (
    select 
      --keys
      id_vacinacao
     from {{ ref('raw_sipni__vacinacao') }}
  )