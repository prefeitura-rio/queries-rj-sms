{{
    config(
        schema='brutos_prontuario_prontuaRio',
        alias="emergencia",
        materialized="table",
        tags=["prontuaRio"],
        partition_by={
            "field": "data_particao",
            "data_type": "date",
            "granularity": "day",
        },
    )
}}

with 

    source_ as (
        select * from {{source('brutos_prontuario_prontuaRio_staging', 'hp_rege_emerg') }}
    ),

    emergencia as (
        select
            json_extract_scalar(data, '$.be') as id_boletim,
            json_extract_scalar(data, '$.num_cor') as numero_cor,
            json_extract_scalar(data, '$.cor') as cor,
            safe_cast(json_extract_scalar(data, '$.tempo_espera') as datetime) as tempo_espera,
            json_extract_scalar(data, '$.nome_pac') as paciente_nome,
            json_extract_scalar(data, '$.motivo') as motivo,
            json_extract_scalar(data, '$.dt_ent_saida') as data_entrada_saida,
            json_extract_scalar(data, '$.cpf_emerg') as cpf_emergencia,
            cnes,
            loaded_at
    from source_
    ),

    final as (
        select 
            {{ process_null('id_boletim') }} as id_boletim,
            {{ process_null('numero_cor') }} as numero_cor,
            {{ process_null('cor') }} as cor,
            tempo_espera,
            {{ process_null('paciente_nome') }} as paciente_nome,
            {{ process_null('motivo') }} as motivo,
            data_entrada_saida,
            {{ process_null('cpf_emergencia') }} as cpf_emergencia,
            cnes,
            loaded_at,
            cast(safe_cast(loaded_at as timestamp) as date) as data_particao
        from emergencia
        qualify row_number() over(partition by id_boletim, cnes order by loaded_at desc) = 1
    )

select * from final