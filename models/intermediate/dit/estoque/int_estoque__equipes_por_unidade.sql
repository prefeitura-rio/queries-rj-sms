--- Quantidade de equipes por unidades

with
    equipe as (select * from {{ source("brutos_cnes_web_staging", "tbEquipe") }} where mes_particao = '2023-11'),  # TODO: remove from staging and hardcoded date

    cnes_por_equipe as (
        select est.id_unidade, est.id_cnes, equipe.*
        from {{ ref("dim_estabelecimento") }} as est
        left join equipe on est.id_unidade = equipe.co_unidade
    ),

    cnes_agrupado_por_tipo_equipe as (
        select id_cnes, tp_equipe, count(1) as equipes_quantidade
        from cnes_por_equipe
        where tp_equipe in ('70', '71', '72', '73', '74', '76', '22', '23') # TODO: uderstand what are these codes and why other are not included
        group by 1,2
        order by id_cnes, tp_equipe
    )

    select id_cnes, sum(equipes_quantidade) as equipes_quantidade
    from cnes_agrupado_por_tipo_equipe
    group by 1

