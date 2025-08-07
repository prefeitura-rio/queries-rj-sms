{{
    config(
        schema="intermediario_historico_clinico",
        alias="vacinacao_api",
        materialized="table",
    )
}}

with

    source_vacina as (
        select 
            id_cnes as cnes_unidade,
            paciente_cpf as cpf,
            paciente_nome as nome,
            paciente_nascimento_data as data_nascimento,
            nome_mae as nome_mae,
            lower({{ remove_accents_upper('descricao') }}) as nome_vacina,
            dose as dose,
            aplicacao_data as data_aplicacao,
            date(registro_data) as data_registro
        from {{ ref('raw_prontuario_vitacare__vacina') }} 
    )

select * from source_vacina 
