{{
    config(
        schema="saude_dados_mestres",
        alias="vinculo_profissional_saude_estabelecimento",
        materialized="table",
        tags=["weekly"]
    )
}}

with
    profissional_serie_historica as (
        select *
        from {{ ref("int_profissional_saude__vinculo_estabelecimento_serie_historica") }}
     ),
    estabelecimentos as (select distinct id_cnes from {{ ref("dim_estabelecimento") }})


select
    p.id_cnes,
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
    data_registro as data_ultima_atualizacao,
    current_date('America/Sao_Paulo') as data_referencia

from profissional_serie_historica as p
inner join estabelecimentos as estabelecimentos 
on p.id_cnes = estabelecimentos.id_cnes
