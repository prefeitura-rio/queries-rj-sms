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

    cadastro as (
        select
            cpf,
            id_cnes
        from {{ ref('raw_prontuario_vitacare__paciente') }}
    ),

    -- no momento, estamos consideramos apenas pacientes com cadastro em unidades da ap 53
    pacientes_ap53 as (
        select distinct
            c.cpf
        from cadastro c
        inner join estabelecimento e using (id_cnes)
        where e.area_programatica = '53'
    ),

    -- Desses com cadastro na 53, pegamos todas as vacinacoes pentavalente
    vacinacao as (
        
        select 
            v.id_cnes,
            v.paciente_nome,
            v.paciente_cpf,
            v.paciente_cns,
            v.paciente_nascimento_data, 
            v.vacina_descricao,
            v.vacina_dose,
            v.vacina_lote,
            v.vacina_aplicacao_data,
            v.vacina_registro_data
        from {{ ref("mart_historico_clinico__vacinacao") }} v
        inner join pacientes_ap53 p on v.paciente_cpf = p.cpf
        where 1=1
        and v.paciente_nascimento_data > date_sub(current_date, interval 7 year)
        and v.vacina_descricao like '%penta%'
        and v.vacina_descricao not like '%soro%'
        and v.vacina_descricao not like '%rota%'
    )

select * from vacinacao