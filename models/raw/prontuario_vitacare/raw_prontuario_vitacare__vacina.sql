{{
    config(
        alias="vacina",
        materialized="table",
        tags=['daily'],
        partition_by={
            "field": "data_particao",
            "data_type": "date",
            "granularity": "day",
        },
    )
}}

-- dbt run --select raw_prontuario_vitacare__vacina
with
    vacina as (
        select *, 'historico' as tipo,
        from {{ ref("base_prontuario_vitacare__vacina_historico") }}
        union all 
        select *, 'continuo' as tipo,
        from {{ ref("base_prontuario_vitacare__vacina_continuo") }}
        union all 
        select *, 'api' as tipo,
        from {{ ref("base_prontuario_vitacare__vacina_api") }}
    ),

    padroniza_vacinas as (
        select
            -- PK
            {{ remove_accents_upper("id") }} as id,
            {{ remove_accents_upper("id_cnes") }} as id_cnes,
            {{
                dbt_utils.generate_surrogate_key(
                    [
                        "id_cnes",
                        "id",
                        "dose"
                    ]
                )
            }} as id_surrogate,

            {{ remove_accents_upper("nome_vacina") }} as nome_vacina,
            {{ remove_accents_upper("dose") }} as dose,
            data_aplicacao as aplicacao_data,
            data_registro as registro_data,
            {{ remove_accents_upper("diff") }} as diff,
            {{ remove_accents_upper("lote") }} as lote,
            {{ remove_accents_upper("tipo_registro") }} as registro_tipo,
            {{ remove_accents_upper("estrategia_imunizacao") }} as estrategia_imunizacao,
            data_particao as data_particao,
            loaded_at as loaded_at,

            updated_at_rank

        from vacina
    ),

    vacina_deduplicado as (
        select *,
        from padroniza_vacinas
        qualify
            row_number() over (
                partition by id_surrogate order by updated_at_rank desc
            ) = 1
    )

select *
from vacina_deduplicado
