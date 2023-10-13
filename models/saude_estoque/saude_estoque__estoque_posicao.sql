{{
    config(
        alias="estoque_posicao",
        schema="saude_estoque",
        labels={"contains_pii": "no"},
        materialized="table",
        partition_by={
            "field": "data_particao",
            "data_type": "date",
            "granularity": "month",
        },
    )
}}

with
    posicao_vitai as (
        select *, "vitai" as sistema_origem
        from {{ ref("brutos_prontuario_vitai__estoque_posicao") }}
    ),

    posicao_tpc as (
        select "-" as id_cnes, *, "tpc" as sistema_origem
        from {{ ref("brutos_estoque_central_tpc__estoque_posicao") }}
    )

select *
from posicao_vitai
union all
select *
from posicao_tpc
