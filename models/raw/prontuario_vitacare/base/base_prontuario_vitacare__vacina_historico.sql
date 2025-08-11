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
            concat(id_prontuario_global, '.', nome_vacina) as id,
            id_cnes as id_cnes,

            nome_vacina as nome_vacina,
            case
              when dose = '1ª Dose' then '1 dose'
              when dose = '2ª Dose' then '2 dose'
              when dose = '3ª Dose' then '3 dose'
              when dose = '4ª Dose' then '4 dose'
              when dose = '5ª Dose' then '5 dose'
              when dose = 'Dose única' then 'dose unica'
              when dose = 'Dose adicional' then 'dose adicional'
              when dose = 'Dose inicial' then 'dose inicial'
              when dose = 'Revacinação' then 'revacinacao'
              when dose = 'Reforço' then 'reforco'
              when dose = '1º Reforço' then '1 reforco'
              when dose = '2º Reforço' then '2 reforco'
              when dose = '3º Reforço' then '3 reforco'
              when dose = 'Dose D' then 'dose d'
              when dose = 'Outro' then 'outra'
              else lower(dose) 
            end as dose,
            data_aplicacao as data_aplicacao,
            data_registro as data_registro,
            diff as diff,
            lote as lote,

            tipo_registro as tipo_registro,
            estrategia_imunizacao as estrategia_imunizacao,

            data_particao as data_particao,
            (safe_cast(loaded_at as datetime)) as loaded_at,

            greatest(
                safe_cast(data_aplicacao as timestamp),
                safe_cast(data_registro as timestamp)
            ) as updated_at_rank

        from source_vacina
    )

select
    *
from selecao_vacinas