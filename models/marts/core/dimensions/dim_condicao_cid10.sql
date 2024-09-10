{{
    config(
        alias="condicao_cid10",
        labels={
            "dado_publico": "sim",
            "dado_pessoal": "nao",
            "dado_anonimizado": "nao",
            "dado_sensivel_saude": "nao",
        },
    )
}}

with
    source as (select * from {{ ref("raw_datasus__cid10") }}),

    grupo_agrupado as (
        select
            id_subcategoria,
            subcategoria_descricao,
            ordem,
            id_categoria,
            categoria_descricao,
            categoria_descricao_abv,
            id_capitulo,
            capitulo_descricao,
            array_agg(
                struct(
                    safe_cast(grupo_descricao as string) as descricao,
                    safe_cast(grupo_descricao_abv as string) as abreviatura,
                    safe_cast(grupo_descricao_len as int) as comprimento
                )
            ) as grupo
        from source
        group by 1,2,3,4,5,6,7,8),

    final as (

        select
            safe_cast(id_subcategoria as string) as id,
            safe_cast(subcategoria_descricao as string) as descricao,
            safe_cast(ordem as int) as ordem,

            struct(
                safe_cast(id_categoria as string) as id,
                safe_cast(categoria_descricao as string) as descricao,
                safe_cast(categoria_descricao_abv as string) as abreviatura
            ) as categoria,

            struct(
                safe_cast(id_capitulo as string) as id,
                safe_cast(capitulo_descricao as string) as descricao
            ) as capitulo,

            grupo

        from grupo_agrupado
        order by ordem, id

    )

select *
from final
