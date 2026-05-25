{{
    config(
        schema="brutos_prontuario_vitacare_historico",
        alias="vacina", 
        materialized="incremental",
        unique_key = ['id_prontuario_global', 'id_vacinacao'],
        cluster_by= ['id_prontuario_global', 'id_vacinacao'],
        partition_by={
            "field": "data_particao",
            "data_type": "date",
            "granularity": "day"
        }
    )
}}

{% set last_partition = get_last_partition_date(this) %}

with

   vacina_source as (
        select
            id,
            acto_id,
            prof_id,
            ut_id,
            nome_vacina,
            cod_vacina,
            dose,
            lote,
            data_aplicacao,
            data_registro,
            diff,
            calendario_vacinal_atualizado,
            tipo_registro,
            estrategia_imunizacao,
            foi_aplicada,
            justificativa,
            extracted_at,
            id_cnes,
            ano_particao,
            mes_particao,
            data_particao
        from {{ source('brutos_prontuario_vitacare_historico_staging', 'vacinas') }} 
        {% if is_incremental() %}
            where data_particao > '{{last_partition}}'
        {% endif %}
    ),

     vacina_dedup as (
        select
            *
        from vacina_source 
        qualify row_number() over (partition by id_cnes, acto_id, id order by extracted_at desc) = 1
    ),

    vacina_casted as (
        select
             -- Keys
           {{ process_null(
                "id_cnes || '.' || replace(acto_id, '.0', '')"
            ) }} as id_prontuario_global,

            {{ process_null("replace(acto_id, '.0', '')") }} as id_prontuario_local,

            {{ process_null(
                "id_cnes || '.' || replace(ut_id, '.0', '')"
            ) }} as id_cadastro,
            
            {{ process_null(
                "id_cnes || '.' || replace(prof_id, '.0', '')"
            ) }} as id_profissional,

            {{ process_null('id_cnes') }} as id_cnes,

            {{ process_null(
                "id_cnes || '.' || id"
            ) }} as id_vacinacao,

            -- Variables
            {{ process_null('nome_vacina') }} as nome_vacina,
            {{ process_null('cod_vacina') }} as cod_vacina,
            {{ process_null('dose') }} as dose,
            {{ process_null('lote') }} as lote,

            case
                when cast(substr({{ process_null('data_aplicacao') }}, 1, 10) as date) <= date '1900-01-01'
                    then null
                else cast(substr({{ process_null('data_aplicacao') }}, 1, 10) as date)
            end as data_aplicacao,
            cast(substr({{ process_null('data_registro') }}, 1, 10) as date) as data_registro,
            {{ process_null("replace(diff, '.0', '')") }} as diff,

            {{ process_null('calendario_vacinal_atualizado') }} as calendario_vacinal_atualizado,
            {{ process_null('tipo_registro') }} as tipo_registro,
            {{ process_null('estrategia_imunizacao') }} as estrategia_imunizacao,
            {{ process_null('foi_aplicada') }} as foi_aplicada,
            {{ process_null('justificativa') }} as justificativa,

             -- Metadata
            datetime(cast({{ process_null('extracted_at') }} as timestamp), 'America/Sao_Paulo') as loaded_at,
            cast({{ process_null('data_particao') }} as date) as data_particao,
          
        from vacina_dedup
    )

select 
    *
from vacina_casted



