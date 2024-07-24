-- Teste se a posição calculada bate com a posição obtida pela API
with
    --
    ultima_posicao_calculada as (
        select id_cnes, id_material, data_evento, posicao_final as posicao_calculada,
        from
            (
                select
                    *,
                    rank() over (
                        partition by id_cnes, id_material order by ordem desc
                    ) rank
                from {{ ref("mart_estoque__report_medicamentos_controlados") }}
            )
        where rank = 1
    ),

    posicao_api as (
        select
            id_cnes,
            id_material,
            date_sub(data_particao, interval 1 day) as data_particao, -- A API retorna a posição do dia anterior
            sum(material_quantidade) as posicao_api,
        from {{ ref("fct_estoque_posicao") }}
        where
            data_particao >= date_sub(
                current_date('America/Sao_Paulo'),
                interval {{ dbt_date.day_of_month("current_date('America/Sao_Paulo')") }} -1 day
            )
            and sistema_origem = "vitacare"
        group by 1, 2, 3
        order by 3
    )

select calculada.*, api.posicao_api
from ultima_posicao_calculada as calculada
left join
    posicao_api as api
    on calculada.id_cnes = api.id_cnes
    and calculada.id_material = api.id_material
    and calculada.data_evento = api.data_particao
where calculada.posicao_calculada <> api.posicao_api
    
