{{
    config(
        schema="brutos_centralderegulacao_mysql",
        alias="minha_saude__lista_usuario",
        partition_by={
            "field": "data_particao",
            "data_type": "date",
            "granularity": "month",
        },
    )
}}

with
    source as (
        select *
        from
            {{
                source(
                    "brutos_centralderegulacao_mysql_staging",
                    "vw_minhaSaude_listaUsuario",
                )
            }}
    ),
    final as (select * from source)
select *
from final
