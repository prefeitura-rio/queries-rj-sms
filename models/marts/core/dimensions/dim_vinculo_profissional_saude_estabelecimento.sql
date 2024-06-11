{{
    config(
        schema="saude_dados_mestres",
        alias="vinculo_profissional_saude_estabelecimento",
        materialized="table",
    )
}}

with
    profissional_serie_historica as (
        select *
        from
            {{ ref("int_profissional_saude__vinculo_estabelecimento_serie_historica") }}

    )

select
    id_cnes,
    profissional_codigo_sus as id_profissional_sus,
    profissional_cns,
    id_cbo,
    cbo as cbo_nome,
    id_cbo_familia,
    cbo_familia as cbo_familia_nome,
    id_registro_conselho,
    id_tipo_conselho,
    vinculacao,
    vinculo_tipo,
    carga_horaria_outros,
    carga_horaria_hospitalar,
    carga_horaria_ambulatorial,
    data_registro as data_ultima_atualizacao

from profissional_serie_historica
where
    data_registro = (
        select distinct data_registro
        from profissional_serie_historica
        order by 1 desc
        limit 1
    )
