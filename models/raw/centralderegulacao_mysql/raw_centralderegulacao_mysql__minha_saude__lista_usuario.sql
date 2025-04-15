{{
    config(
        schema="brutos_centralderegulacao_mysql", alias="minha_saude__lista_usuario"
    )
}}

with
    source as (
        select *
        from
            {{
                source(
                    "brutos_centralderegulacao_mysql_staging",
                    "monitoramento__vw_minhaSaude_listaUsuario",
                )
            }}
    ),
    deduplicated as (
        select *
        from source
        qualify row_number() over (partition by idusuario order by datahoracadastro desc) = 1
    )
select *
from deduplicated
