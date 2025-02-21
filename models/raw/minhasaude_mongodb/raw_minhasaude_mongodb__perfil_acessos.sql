{{ config(alias="perfil_acessos") }}


with
    source as (
        select *
        from
            {{
                source(
                    "brutos_minhasaude_mongodb_staging", "perfil_acessos"
                )
            }}
    )
select *
from source