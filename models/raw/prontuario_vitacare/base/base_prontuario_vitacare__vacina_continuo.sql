{{
    config(
        schema="brutos_prontuario_vitacare_staging",
        alias="_base_vacina_continuo",
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
        from {{ ref('raw_prontuario_vitacare_api__vacina') }} 
        {% if is_incremental() %} where loaded_at > '{{seven_days_ago}}' {% endif %}
    ),

    selecao_vacinas as (
        select
            -- PK
            concat(id_prontuario_global, '.', nome_vacina) as id,
            id_cnes as id_cnes,

            nome_vacina as nome_vacina,
            case
              when dose = 'Single Dose' then 'dose unica'
              when dose = '1st Dose' then '1 dose'
              when dose = '2nd Dose' then '2 dose'
              when dose = '3rd Dose' then '3 dose'
              when dose = '4th Dose' then '4 dose'
              when dose = '5th Dose' then '5 dose'
              when dose = 'Booster' then 'reforco'
              when dose = '1st Booster' then '1 reforco'
              when dose = '2nd Booster' then '2 reforco'
              when dose = '3rd Booster' then '3 reforco'
              when dose = 'Re-Vaccination' then 'revacinacao'
              when dose = 'Dose D' then 'dose d'
              else lower(dose)
            end as dose,
            data_aplicacao as data_aplicacao,
            data_registro as data_registro,
            diff as diff,
            lote as lote,

            tipo_registro as tipo_registro,
            estrategia_imunizacao as estrategia_imunizacao,

            data_particao as data_particao,
            loaded_at as loaded_at,

            greatest(
                safe_cast(data_aplicacao as timestamp),
                safe_cast(data_registro as timestamp)
            ) as updated_at_rank

        from source_vacina
    )

select
    *
from selecao_vacinas