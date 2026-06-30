{{
    config(
        alias="vacinacao",
        schema="app_historico_clinico_treinamento",
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
        "2277271.54321" as id_vacinacao,
        2277271 as id_cnes,
        "Inst de Medicina Veterinária Jorge Vaitsman" as estabelecimento_nome,
        "Pedro Marques" as profissional_nome,
        "Antirrábica" as vacina_descricao,
        cast(null as string) as vacina_sigla,
        cast(null as string) as vacina_detalhes,
        "1 dose" as vacina_dose,
        "L073F4K3" as vacina_lote,
        "administracao" as vacina_registro_tipo,
        "pos-exposicao" as vacina_estrategia,
        date("2025-01-31") as vacina_aplicacao_data,
        date("2025-02-02") as vacina_registro_data,
        42298037299 as cpf_particao
    union all
    select
        "2277301.001122" as id_vacinacao,
        2277301 as id_cnes,
        "CMS Manoel Arthur Villaboim" as estabelecimento_nome,
        "Tarsila de Aguiar do Amaral" as profissional_nome,
        "Pneumocócica 13-valente" as vacina_descricao,
        "Pncc13V" as vacina_sigla,
        "S. pneumoniae" as vacina_detalhes,
        "Reforço" as vacina_dose,
        "1OT3R34L" as vacina_lote,
        "registro de vacinacao anterior" as vacina_registro_tipo,
        cast(null as string) as vacina_estrategia,
        date("2025-06-07") as vacina_aplicacao_data,
        date("2025-07-08") as vacina_registro_data,
        42298037299 as cpf_particao
)

select *
from vacinacao
