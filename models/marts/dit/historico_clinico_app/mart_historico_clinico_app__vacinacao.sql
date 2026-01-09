{{
    config(
        alias="vacinacao",
        schema="app_historico_clinico",
        materialized="table",
        partition_by={
            "field": "cpf_particao",
            "data_type": "int64",
            "range": {"start": 0, "end": 100000000000, "interval": 34722222},
        },
    )
}}

with vacinacao as (
    select
        id_vacinacao,
        id_cnes,
        estabelecimento_nome,
        profissional_nome,
        vacina_descricao_padronizada as vacina_descricao,
        vacina_sigla,
        vacina_detalhes,
        vacina_dose,
        vacina_lote,
        vacina_categoria,
        vacina_registro_tipo,
        vacina_estrategia,
        vacina_aplicacao_data,
        vacina_registro_data,
        cpf_particao
    from {{ ref('mart_historico_clinico__vacinacao') }}
)

select *
from vacinacao
