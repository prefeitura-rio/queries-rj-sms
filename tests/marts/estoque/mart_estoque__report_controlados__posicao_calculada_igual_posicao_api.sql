-- Teste se a posição calculada bate com a posição obtida pela API
{{ config(
    error_if = ">2",
    warn_if = ">0",    
) }}


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
                from {{ ref("mart_estoque__report_medicamentos_controlados__itens_com_movimento") }}
            )
        where rank = 1
    ),

    posicao_calculada as (
        select
            id_cnes,
            id_material,
            date_add(data_evento, interval 1 day) as data_estoque_inicio_proximo_dia,
            posicao_calculada
        from ultima_posicao_calculada
    ),

    posicao_api as (
        select
            id_cnes,
            id_material,
            data_particao,
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
from posicao_calculada as calculada
left join
    posicao_api as api
    on calculada.id_cnes = api.id_cnes
    and calculada.id_material = api.id_material
    and calculada.data_estoque_inicio_proximo_dia = api.data_particao
where calculada.posicao_calculada != api.posicao_api
