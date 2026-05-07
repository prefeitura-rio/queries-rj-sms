{{
    config(
        schema="brutos_prontuario_vitacare_historico",
        alias="vacina", 
        materialized="incremental",
        unique_key = ['id_global', 'id_vacinacao'],
        cluster_by = ['id_global', 'id_vacinacao'],
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

    vacina_cast as (
        select
            -- Keys
           {{ process_null(
                "id_cnes || '.' || replace(acto_id, '.0', '')"
            ) }} as id_global,

            {{ process_null("replace(acto_id, '.0', '')") }} as id_local,

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
            {{ process_null('nome_vacina') }} as vacina_nome,
            {{ process_null('cod_vacina') }} as vacina_codigo,
            {{ process_null('dose') }} as vacina_dose,
            {{ process_null('lote') }} as vacina_lote,

            cast(substr({{ process_null('data_aplicacao') }}, 1, 10) as date) as vacina_aplicacao_data,
            cast(substr({{ process_null('data_registro') }}, 1, 10) as date) as vacina_registro_data,
            {{ process_null("replace(diff, '.0', '')") }} as vacina_diferenca_dias,

            cast({{ process_null('calendario_vacinal_atualizado') }}as boolean) as vacina_calendario_atualizado,
            {{ process_null('tipo_registro') }} as vacina_tipo_registro,
            {{ process_null('estrategia_imunizacao') }} as vacina_estrategia_imunizacao,
            {{ process_null('foi_aplicada') }} as vacina_foi_aplicada,
            {{ process_null('justificativa') }} as vacina_justificativa,

            -- Metadata
            cast({{ process_null('extracted_at') }} as datetime) as loaded_at,
            cast({{ process_null('data_particao') }} as date) as data_particao,
            
        from vacina_dedup
    )

select * from vacina_cast

