{{
    config(
        alias="int_gdb_cnes__vinculo_detalhe",
        materialized="table",
        tags=["gdb_cnes"]
    )
}}

with
    vinculo as (
        select distinct
            id_vinculo,
            id_vinculacao,
            tipo_vinculo,
            tipo_subvinculo,
            descricao_subvinculo,
            descricao_conceito,
            habilitado, 
            solicita_cnpj
        from {{ ref("raw_gdb_cnes__vinculo_detalhe") }}
    ),

    vinculo_empregador as (
        select distinct 
            id_vinculacao,
            id_tipo_vinculo,
            descricao_vinculo
        from {{ ref("raw_gdb_cnes__vinculo_empregador") }}
    ),

    vinculo_estabelecimento as (
        select distinct
            id_vinculacao,
            descricao_vinculacao
        from {{ ref("raw_gdb_cnes__vinculo_estabelecimento") }}
    )

select distinct
    id_vinculo,
    id_vinculacao,
    descricao_vinculo as vinculo,
    descricao_subvinculo,
    descricao_conceito,
    descricao_vinculacao as vinculacao,
    habilitado, 
    solicita_cnpj
from vinculo
left join vinculo_empregador using (id_vinculacao)
left join vinculo_estabelecimento using (id_vinculacao)