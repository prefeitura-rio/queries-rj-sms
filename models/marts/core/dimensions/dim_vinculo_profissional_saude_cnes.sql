{{
    config(
        schema="saude_dados_mestres",
        alias="vinculacao_profissional",
        materialized="table",
    )
}}


select
    id_estabelecimento_cnes,
    nome_profissional,
    cod_profissional_sus,
    cartao_nacional_saude,
    cbo_descricao,
    cbo_fam_descricao,
    vinculacao_descricao,
    tipo_vinculo_descricao,
    carga_horaria_outros,
    carga_horaria_hospitalar,
    carga_horaria_ambulatorial,
    id_registro_conselho,
    tipo_conselho,
    data_registro as data_ultima_atualizacao

from {{ ref("int_profissional__vinculo_cnes_serie_historica") }}
where
    data_registro = (
        select distinct data_registro
        from {{ ref("int_profissional__vinculo_cnes_serie_historica") }}
        order by 1 desc
        limit 1
    )
