{{
    config(
        schema="intermediario_historico_clinico",
        alias="alergias_vitacare",
        materialized="table",
    )
}}

with
    alergias_bruto as (
        select
            cpf,
            alergias_anamnese
        from {{ref('raw_prontuario_vitacare__atendimento')}}
    ),
    alergias as (
        select 
            cpf,
            initcap(json_extract_scalar(alergia, '$.descricao')) as alergia_nome
        from alergias_bruto, 
            unnest(json_extract_array(alergias_anamnese)) as alergia
    ),
    alergias_agg as (
        select
            cpf,
            array_agg(alergia_nome) as alergias
        from alergias
        group by cpf
    )
select 
    *
from alergias_agg