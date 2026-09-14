{{
    config(
        schema="intermediario_gdb_sih",
        alias="codigo_descricao",
        materialized="table",
        tags=["gdb_sih"]
    )
}}


with
    dedup as (
        select
            codigo_tabela,
            codigo_item,

            parse_date("%Y%m", nullif(competencia_inicio, "999999")) as competencia_inicio,
            parse_date("%Y%m", nullif(competencia_fim, "999999")) as competencia_fim,

            descricao, -- FIXME: consertar os mil '�'

            data_particao,
            data_carga

        from {{ ref("raw_gdb_sih__c_d") }}
        where data_particao = (
            select max(data_particao)
            from {{ ref("raw_gdb_sih__c_d") }}
        )
        qualify row_number() over (
            partition by codigo_tabela, codigo_item
            order by data_carga desc
        ) = 1
    )

select *
from dedup
order by codigo_tabela, codigo_item
