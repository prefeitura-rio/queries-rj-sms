{{ config(alias="modulos_perfil_acessos") }}


with
    source as (
        select *
        from
            {{
                source(
                    "brutos_minhasaude_mongodb_staging", "modulos_perfil_acessos"
                )
            }}
    )
select *
from source