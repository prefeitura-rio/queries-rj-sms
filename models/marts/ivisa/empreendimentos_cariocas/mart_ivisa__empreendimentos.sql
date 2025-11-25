{{
    config(
        alias="empreendimentos",
        materialized="table",
    )
}}

select * 
from {{ ref('int_ivisa__estabelecimento') }}

union all

select * 
from {{ ref('int_ivisa__feirante') }}

union all

select * 
from {{ ref('int_ivisa__banca_jornal') }}

union all

select * 
from {{ ref('int_ivisa__ambulante') }}

