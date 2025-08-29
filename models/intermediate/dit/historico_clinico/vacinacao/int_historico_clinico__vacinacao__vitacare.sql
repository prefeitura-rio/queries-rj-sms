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
        from {{ref('raw_prontuario_vitacare__vacina')}}
    ),

    episodio as (
        select 
            id_prontuario_global as id,
            cpf
        from {{ref('raw_prontuario_vitacare__atendimento')}}
    ),
    agregado as (
        select 
            v.*,
            e.cpf 
        from vacinas_bruto v
        left join episodio e on rtrim(v.id_vacinacao, concat('.', v.vacina_descricao)) = e.id

    ),
    vacinacoes as (
        select
            id_vacinacao as id, 
            id_surrogate,
            id_cnes,
            cpf,
            vacina_descricao as nome_vacina,
            vacina_dose as dose,
            vacina_aplicacao_data as aplicacao_data,
            vacina_registro_data as registro_data,
            vacina_diff as diff,
            vacina_lote as lote,
            vacina_registro_tipo as registro_tipo,
            vacina_estrategia as estrategia_imunizacao,
            metadados.loaded_at as loaded_at
        from agregado
    )
select 
    *
from vacinacoes

