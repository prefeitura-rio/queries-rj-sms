-- - Monta a relação de unidades & datas que falharam na ingesão para futuro
-- reprocesamento
{{
    config(
        alias="brutos_prontuario_vitacare__estoque_movimento",
        materialized="incremental",
    )
}}

with
    unidades as (
        select id_cnes
        from {{ ref("dim_estabelecimento") }}
        where prontuario_versao = 'vitacare' and prontuario_estoque_tem_dado = 'sim'
    ),

    calendario as (
        select *
        from
            unnest(
                generate_date_array(
                    '2023-10-28',  -- - data de quando começamos a ingestão vitacare
                    date_sub(current_date(), interval 1 day),
                    interval 1 day
                )
            ) as data
    ),

    calendario_sem_domingo as (
        select *, extract(dayofweek from data) as dia_da_semana
        from calendario
        where extract(dayofweek from data) != 1
    ),

    relacao_unidades_datas as (
        select id_cnes, data from unidades cross join calendario_sem_domingo as cal
    ),

    relacao_unidades_datas_com_dados as (
        select distinct id_cnes, data_particao
        from {{ ref("raw_prontuario_vitacare__estoque_movimento") }}
    )

select
    rel.id_cnes,
    rel.data,
    "pending" as reprocessing_status,
    "" as request_response_code,
    "" as request_row_count
from relacao_unidades_datas as rel
left join
    relacao_unidades_datas_com_dados as rel_dados
    on rel.id_cnes = rel_dados.id_cnes
    and rel.data = rel_dados.data_particao
where
    rel_dados.id_cnes is null
    {% if is_incremental() %} and data > (select max(data) from {{ this }}) {% endif %}
