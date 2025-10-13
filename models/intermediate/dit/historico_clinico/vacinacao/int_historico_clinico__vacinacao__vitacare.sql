{{
    config(
        schema="intermediario_historico_clinico",
        alias="vacinacao_vitacare",
        materialized="table",
    )
}}

with
    vacinas_bruto as (
        select *
        from {{ref('raw_prontuario_vitacare__vacinacao')}}
    ),

    vacinacoes as (
        select
            id_vacinacao as id, 
            id_surrogate,
            id_cnes,
            paciente_cpf as cpf,
            vacina_descricao as nome_vacina,
            vacina_dose as dose,
            vacina_aplicacao_data as aplicacao_data,
            vacina_registro_data as registro_data,
            vacina_diff as diff,
            vacina_lote as lote,
            vacina_registro_tipo as registro_tipo,
            vacina_estrategia as estrategia_imunizacao,
            metadados.loaded_at as loaded_at
        from vacinas_bruto
    )
select 
    *
from vacinacoes

