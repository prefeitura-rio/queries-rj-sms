{{
    config(
        schema = 'intermediario_gdb_cnes',
        alias="estabelecimento",
        materialized="table",
        tags=["gdb_cnes"],
        partition_by={
            "field": "data_particao",
            "data_type": "date",
            "granularity": "month",
        },
    )
}}

with base as (

    select
        * except(data_particao),
        safe_cast(data_particao as date) as data_particao
    from {{ ref('raw_gdb_cnes__estabelecimento') }}

),

estabelecimento as (

    select *
    from base
    where data_particao = (select max(data_particao) from base)

),

final as (

    select
        estabelecimento.*,

        regexp_extract(
            upper(trim(nome_fantasia)),
            r'^(SMS(?:\s+RIO)?|SES\s+RJ|MS|UFRJ|UERJ\s+HUPE|FIOTEC\s+IFF)\b'
        ) as sigla,

        nullif(
            {{ remove_duplicate_whitespace(
                "trim(regexp_replace(regexp_replace(upper(trim(nome_fantasia)), r'^(SMS(?:\\s+RIO)?|SES\\s+RJ|MS|UFRJ|UERJ\\s+HUPE|FIOTEC\\s+IFF)\\b[\\s\\-–:/]*', ''), r'\\s*-?\\s*AP\\s*\\d{1,2}$', ''))"
            ) }},
            ''
        ) as nome_limpo,

        nullif(trim(id_distrito_sanitario), '') as area_programatica

    from estabelecimento

)

select *
from final