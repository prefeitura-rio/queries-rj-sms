-- - Monta a relação de unidades & datas que falharam na ingesão para futuro
-- reprocesamento
{{
    config(
        alias="brutos_prontuario_vitacare__vacina",
        schema="gerenciamento__reprocessamento",
        materialized="incremental",
        incremental_strategy = 'merge',
        unique_key=["id_cnes", "data"],
    )
}}

with
    -- RELAÇÃO DE UNIDADE & DATAS
    unidades as (
        select id_cnes, area_programatica, nome_limpo
        from {{ ref("dim_estabelecimento") }}
        where prontuario_versao = 'vitacare' and prontuario_episodio_tem_dado = 'sim'
    ),

    calendario as (
        select *
        from
            unnest(
                generate_date_array(
                    '2024-11-01',  -- data de quando começamos a ingestão vitacare
                    date_sub(current_date('America/Sao_Paulo'), interval 3 day), -- 4 dias atrás (d-4), dado que o flow rotineiro roda d-3
                    interval 1 day
                )
            ) as data
    ),

    -- calendario_sem_domingo as (
    --     select *, extract(dayofweek from data) as dia_da_semana from calendario
    -- ),  -- atentaçõa primaria não funciona no domingo

    relacao_unidades_datas as (
        select id_cnes, area_programatica, nome_limpo, data
        from unidades
        cross join calendario as cal
    ),

    -- RELAÇÃO DE UNIDADES COM DADOS INGERIDOS
    relacao_unidades_datas_com_dados as (
        select distinct id_cnes, particao_data_vacinacao as data
        from {{ ref("raw_prontuario_vitacare__vacinacao") }}
    ),

    -- RELAÇÃO DE UNIDADES QUE JÁ FORAM REPROCESSADAS E NÃO RETORNARAM REGISTROS OU
    -- QUE ESTÃO REPROCESSANDO
    relacao_unidades_reprocessadas as (
        select *
        from {{ this }}
        where
            (retry_status = "finished" and request_row_count = 0)
            or retry_status = "in progress"
    ),

    final as (

        select
            rel.id_cnes,
            rel.area_programatica,
            rel.nome_limpo,
            rel.data,
            "pending" as retry_status,
            0 as retry_attempts_count,
            safe_cast("" as int64) as request_row_count
        from relacao_unidades_datas as rel
        left join relacao_unidades_datas_com_dados as rel_dados using (id_cnes, data)
        left join
            relacao_unidades_reprocessadas as rel_reprocessadas using (id_cnes, data)
        where
            rel_dados.id_cnes is null  -- não possui dados
            {% if is_incremental() %} 
                and rel_reprocessadas.id_cnes is null  -- não está reprocessado como vazio
            {% endif %}

        order by rel.data, rel.area_programatica, rel.id_cnes
    )

select *
from final

