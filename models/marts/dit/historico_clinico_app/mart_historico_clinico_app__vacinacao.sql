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
        vacina_estrategia,

        case
            when vacina_registro_tipo = "administracao"
                then "Administração"
            when vacina_registro_tipo = "registro de vacinacao anterior"
                then "Registro de vacinação anterior"
            else {{ capitalize_first_letter("vacina_registro_tipo")}}
        end as vacina_registro_tipo,

        vacina_aplicacao_data,
        vacina_registro_data,
        cpf_particao
    from {{ ref('mart_historico_clinico__vacinacao') }}
    where (
        -- Queremos todas as entradas cujo `vacina_registro_tipo`
        -- não seja especificamente "nao aplicada";
        -- Em SQL, ao usar `COL != const`, se COL for NULL,
        -- a expressão inteira vira NULL, que é false-y.
        -- Ou seja, `vacina_registro_tipo != "nao aplicada"`
        -- também remove `vacina_registro_tipo` com valor de NULL

        -- O mais apropriado aqui portanto é `IS DISTINCT FROM`,
        -- que faz o que você esperaria que `!=` fizesse
        vacina_registro_tipo is distinct from "nao aplicada"
    )
    and cpf_particao is not null
)

select *
from vacinacao
