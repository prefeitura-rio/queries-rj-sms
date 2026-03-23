{{
    config(
        schema="subpav_vacinacao",
        alias="pentavalente",
        materialized="table",
        tags=["daily"],
    )
}}

with estabelecimento as (
        select
            id_cnes,
            area_programatica
        from {{ ref('dim_estabelecimento') }}
    ),

    vacinacao as (
        select 
            v.paciente_nome,
            v.paciente_cpf,
            v.paciente_cns,
            v.paciente_nascimento_data, 
            v.vacina_descricao,
            v.vacina_dose,
            v.vacina_aplicacao_data,
            v.vacina_registro_data
        from {{ ref("mart_historico_clinico__vacinacao") }} v
        left join estabelecimento e using (id_cnes)
        where 1=1
        and date_diff(current_date, v.paciente_nascimento_data, year) <= 6
        and v.vacina_descricao like '%penta%'
        and v.vacina_descricao not like '%soro%'
        and v.vacina_descricao not like '%rota%'
        and e.area_programatica = '53'
    )

select * from vacinacao