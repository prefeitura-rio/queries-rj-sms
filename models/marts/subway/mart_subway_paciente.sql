{{ config(
    schema = "projeto_subway",
    alias = "paciente",
    materialized = "table",
    tags = ['daily']
) }}

with base as (
    select *
    from {{ ref('raw_prontuario_vitacare__paciente') }}

),

paciente as (
    select
        cpf,
        nome,
        nome_social,
        data_nascimento,
        sexo,
        orientacao_sexual,
        identidade_genero,
        raca,
        cns,
        mae_nome,
        endereco_cep,
        endereco_estado,
        endereco_municipio,
        endereco_bairro,
        endereco_logradouro,
        endereco_numero,
        endereco_complemento
    from base
)

select *
from paciente