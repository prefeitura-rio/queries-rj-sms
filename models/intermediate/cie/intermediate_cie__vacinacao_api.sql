{{
    config(
        alias="vacinacao_api",
        schema="intermediario_cie",
        materialized="table",
        partition_by={
            "field": "particao_data_vacinacao",
            "data_type": "date",
            "granularity": "month"
        }
    )
}}

with
    source as (
        select * from {{ ref("raw_prontuario_vitacare__vacinacao") }}
    ),

    pacientes as (
        select
            id_vacinacao,
            id_surrogate,
            id_cnes,
            id_equipe,
            id_equipe_ine,
            id_microarea,
            paciente_id_prontuario,
            paciente_cns,
            paciente_cpf,
            estabelecimento_nome,
            equipe_nome,
            profissional_nome,
            profissional_cbo,
            profissional_cns,
            profissional_cpf,
            vacina_descricao,
            vacina_dose,
            vacina_lote,
            vacina_registro_tipo,
            vacina_estrategia,
            vacina_diff,
            vacina_aplicacao_data,
            vacina_registro_data,
            paciente_nome,
            paciente_sexo,
            paciente_nascimento_data,
            paciente_nome_mae,
            paciente_mae_nascimento_data,
            paciente_situacao,
            paciente_cadastro_data,
            paciente_obito,
            requisicao_id_cnes,
            requisicao_area_programatica,
            requisicao_endpoint,
            metadados,
            particao_data_vacinacao
        from source
    )

select * 
from pacientes