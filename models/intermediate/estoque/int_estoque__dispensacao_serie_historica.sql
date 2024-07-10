with
    dispensacao_diaria as (
        select
            id_cnes,
            id_material,
            data_particao,
            sum(material_quantidade) as material_quantidade
        from {{ ref("fct_estoque_movimento") }}
        where movimento_tipo_grupo = "Consumo"
        group by id_cnes, id_material, data_particao
        order by id_cnes, id_material, data_particao
    ),
    calendario as (
        select *
        from
            unnest(
                generate_date_array(
                    '2023-01-01',
                    date_sub(current_date('America/Sao_Paulo'), interval 1 day),
                    interval 1 day
                )
            ) as data
    ),
    posicao as (
        select
            id_cnes,
            id_material,
            data_particao,
            sum(material_quantidade) as material_quantidade
        from {{ ref("fct_estoque_posicao") }}
        group by id_cnes, id_material, data_particao
    ),

    dispensacao_minima as (
        select id_material, consumo_minimo as dispensacao_minima
        from {{ ref("dim_material") }}
    )

select
    mat.id_cnes,
    mat.id_material,
    cal.data,
    disp.material_quantidade as quantidade_dispensada_positiva,  -- casos onde não houve dispensação (=0) aparecem como null
    {{ dbt_date.day_of_week("cal.data") }} as dia_semana,
    pos.material_quantidade as quantidade_estocada,
    coalesce(min.dispensacao_minima, 1) as dispensacao_minima,
    coalesce(
        disp.material_quantidade,
        if(
            pos.material_quantidade >= coalesce(min.dispensacao_minima, 1)
            and {{ dbt_date.day_of_week("cal.data") }} <> 7,  -- tem estoque acima do mínimo dispensável e não é domingo
            0,
            null
        )
    ) as quantidade_dispensada,  -- adiciona 0 para casos onde não houve dispensação mas havia estoque
from calendario as cal
cross join (select distinct id_cnes, id_material from dispensacao_diaria) as mat
left join
    dispensacao_diaria as disp
    on disp.data_particao = cal.data
    and disp.id_cnes = mat.id_cnes
    and disp.id_material = mat.id_material
left join
    posicao as pos
    on pos.id_cnes = mat.id_cnes
    and pos.id_material = mat.id_material
    and pos.data_particao = cal.data
left join dispensacao_minima as min on min.id_material = mat.id_material
where
    cal.data >= '2023-01-01'
order by mat.id_cnes desc, mat.id_material, cal.data
