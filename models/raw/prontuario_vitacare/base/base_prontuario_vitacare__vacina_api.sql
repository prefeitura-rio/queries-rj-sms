{{
    config(
        schema="brutos_prontuario_vitacare_staging",
        alias="_base_vacina_api",
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
        from {{ source("brutos_prontuario_vitacare_staging", "vacina") }}
        {% if is_incremental() %} where _data_carga > '{{seven_days_ago}}' {% endif %}
    ),

    selecao_vacinas as (

        select
            -- PK
            concat(ncnesunidade, ".", id) as id,
            {{clean_numeric("id_cnes")}} as id_cnes,

            lower(vacina) as nome_vacina,
            lower(dosevtc) as dose,
            safe_cast(dataaplicacao as date) as data_aplicacao,
            timestamp_add(datetime(safe_cast({{process_null('datahoraregistro')}} as timestamp), 'America/Sao_Paulo'),interval 3 hour) as data_registro,
            diff,
            lote,

            lower(tipo_registro) as tipo_registro,
            lower(estrategia) as estrategia_imunizacao,


            -- Metadata
            safe_cast(data_particao as date) as data_particao,
            timestamp_add(datetime(timestamp({{process_null('imported_at')}}), 'America/Sao_Paulo'),interval 3 hour) as imported_at,

            greatest(
                safe_cast(data_aplicacao as timestamp),
                safe_cast(data_registro as timestamp)
            ) as updated_at_rank

        from source_vacina
    )

select distinct * 
from selecao_vacinas
