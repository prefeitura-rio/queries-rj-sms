{{
    config(
        schema = 'intermediario_gdb_cnes',
        alias="vinculo_detalhe",
        materialized="table",
        tags=["gdb_cnes"]
    )
}}

with
    vinculo as (
        select distinct
            id_vinculo,
            id_vinculacao,
            id_tipo_vinculo,
            id_tipo_subvinculo,
            descricao_subvinculo,
            descricao_conceito,
            habilitado, 
            solicita_cnpj
        from {{ ref("raw_gdb_cnes__vinculo_detalhe") }}
        where data_particao = (select max(data_particao) from {{ ref("raw_gdb_cnes__vinculo_detalhe") }})
    ),

    vinculo_empregador as (
        select distinct 
            id_vinculacao,
            id_tipo_vinculo,
            descricao_vinculo
        from {{ ref("raw_gdb_cnes__vinculo_empregador") }}
        where data_particao = (select max(data_particao) from {{ ref("raw_gdb_cnes__vinculo_empregador") }})
    ),

    vinculo_estabelecimento as (
        select distinct
            id_vinculacao,
            descricao_vinculacao
        from {{ ref("raw_gdb_cnes__vinculo_estabelecimento") }}
        where data_particao = (select max(data_particao) from {{ ref("raw_gdb_cnes__vinculo_estabelecimento") }})
    )

select
    id_vinculo,
    id_vinculacao,
    id_tipo_vinculo,
    id_tipo_subvinculo,
    descricao_vinculo,
    descricao_subvinculo,
    descricao_conceito,
    descricao_vinculacao,
    habilitado, 
    solicita_cnpj
from vinculo
left join vinculo_empregador using (id_tipo_vinculo, id_vinculacao)
left join vinculo_estabelecimento using (id_vinculacao)