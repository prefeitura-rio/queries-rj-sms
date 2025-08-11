{{
    config(
        schema="brutos_prontuario_vitacare_staging",
        alias="_base_vacina_historico",
        materialized="incremental",
        incremental_strategy='merge', 
        unique_key="id",
        partition_by={
            "field": "data_particao",
            "data_type": "date",
            "granularity": "day",
        },
        tags=['weekly']
    )
}}
{% set seven_days_ago = (
    modules.datetime.date.today() - modules.datetime.timedelta(days=7)
).isoformat() %}

with

    source_vacina as (
        select
            *
        from {{ ref('raw_prontuario_vitacare_historico__vacina') }} 
        {% if is_incremental() %} where loaded_at > '{{seven_days_ago}}' {% endif %}
    ),

    selecao_vacinas as (
        select
            -- PK
            id_prontuario_global as id,

            -- Outras Chaves
            id_cnes,
            acto_id as id_local,
            npront as numero_prontuario,

            nome_vacina as nome_vacina,
            cod_vacina as cod_vacina,
            dose as dose,
            lote as lote,
            data_aplicacao as data_aplicacao,
            data_registro as data_registro,
            diff as diff,
            calendario_vacinal_atualizado as calendario_vacinal_atualizado,
            tipo_registro as tipo_registro,
            estrategia_imunizacao as estrategia_imunizacao,
            foi_aplicada as foi_aplicada,
            justificativa as justificativa,

            data_particao as data_particao,
            loaded_at as datalake_imported_at,

            greatest(
                safe_cast(data_aplicacao as timestamp),
                safe_cast(data_registro as timestamp)
            ) as updated_at_rank

        from source_vacina
    )

select
    *
from selecao_vacinas