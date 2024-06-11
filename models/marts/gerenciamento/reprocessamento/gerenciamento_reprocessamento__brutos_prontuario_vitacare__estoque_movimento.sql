-- - Monta a relação de unidades & datas que falharam na ingesão para futuro
-- reprocesamento
{{
    config(
        alias="brutos_prontuario_vitacare__estoque_movimento",
        schema="gerenciamento__reprocessamento",
        materialized="incremental",
    )
}}

with
    unidades as (
        select id_cnes, area_programatica
        from {{ ref("dim_estabelecimento") }}
        where prontuario_versao = 'vitacare' and prontuario_estoque_tem_dado = 'sim'
    ),

    calendario as (
        select *
        from
            unnest(
                generate_date_array(
                    '2024-02-01',  -- - data de quando começamos a ingestão vitacare
                    date_sub(current_date('America/Sao_Paulo'), interval 1 day),
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
        select id_cnes, area_programatica, data from unidades cross join calendario_sem_domingo as cal
    ),

    relacao_unidades_datas_com_dados as (
        select distinct id_cnes, data_particao
        from {{ ref("raw_prontuario_vitacare__estoque_movimento") }}
    )

select
    rel.id_cnes,
    rel.area_programatica,
    rel.data,
    "pending" as retry_status,
    0 as retry_attempts_count,
    safe_cast("" as int64) as request_row_count
from relacao_unidades_datas as rel
left join
    relacao_unidades_datas_com_dados as rel_dados
    on rel.id_cnes = rel_dados.id_cnes
    and rel.data = rel_dados.data_particao
where
    rel_dados.id_cnes is null
    {% if is_incremental() %} and data > (select max(data) from {{ this }}) {% endif %}
order by  rel.data desc, rel.area_programatica, rel.id_cnes
