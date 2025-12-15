{{
    config(
        schema='brutos_prontuario_prontuaRio',
        alias="evolucao",
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
    select * from {{source('brutos_prontuario_prontuaRio_staging', 'hp_rege_evolucao') }}
    ),

    evolucao as (
        select
            json_extract_scalar(data, '$.id') as id_prontuario,
            json_extract_scalar(data, '$.id_be') as id_boletim,
            json_extract_scalar(data, '$.cns') as cns,
            json_extract_scalar(data, '$.registro') as registro, -- process null
            safe_cast(json_extract_scalar(data, '$.data_reg') as datetime) as registro_data,
            safe_cast(json_extract_scalar(data, '$.data_evo') as datetime) as evolucao_data,
            json_extract_scalar(data, '$.profissional') as nome_profissional,
            json_extract_scalar(data, '$.id_profissional') as id_profissional, -- CPF?
            json_extract_scalar(data, '$.descricao') as descricao, 
            json_extract_scalar(data, '$.tipo') as tipo,
            safe_cast(json_extract_scalar(data, '$.data_atu') as datetime) as atualizacao_data,
            json_extract_scalar(data, '$.id_cen38') as id_cen38,
            json_extract_scalar(data, '$.id_am12') as id_am12,
            json_extract_scalar(data, '$.id_cen02') as id_cen02,
            json_extract_scalar(data, '$.id_cen54') as id_cen54,
            json_extract_scalar(data, '$.id_outro') as id_outro,
            json_extract_scalar(data, '$.tip_outro') as tip_outro,
            json_extract_scalar(data, '$.status_evol') as status_evolucao,
            json_extract_scalar(data, '$.ds_sub_atividade') as descricao_sub_atividade,
            json_extract_scalar(data, '$.co_sub_atividade') as id_sub_atividade,
            json_extract_scalar(data, '$.co_atividade') as id_atividade,
            json_extract_scalar(data, '$.codclin') as id_clinica,
            json_extract_scalar(data, '$.setor') as setor,
            json_extract_scalar(data, '$.cid_evo') as cid_evolucao,
            json_extract_scalar(data, '$.cid_descricao') as cid_descricao,
            json_extract_scalar(data, '$.proc_evo') as proc_evo,
            json_extract_scalar(data, '$.proc_descricao') as proc_descricao,
            cnes,
            loaded_at
        from source_
    ),

    final as (
        select 
            {{ process_null('id_prontuario') }} as id_prontuario,
            {{ process_null('id_boletim') }} as id_boletim,
            {{ process_null('cns') }} as cns,
            {{ process_null('registro') }} as registro,
            evolucao_data,
            registro_data,
            {{ process_null('nome_profissional') }} as nome_profissional,
            {{ process_null('id_profissional') }} as id_profissional,
            {{ remove_html('descricao') }} as descricao,
            {{ process_null('tipo') }} as tipo,
            atualizacao_data,
            {{ process_null('id_cen38') }} as id_cen38,
            {{ process_null('id_am12') }} as id_am12,
            {{ process_null('id_cen02') }} as id_cen02,
            {{ process_null('id_cen54') }} as id_cen54,
            {{ process_null('id_outro') }} as id_outro,
            {{ process_null('tip_outro') }} as tip_outro,
            {{ process_null('status_evolucao') }} as status_evolucao,
            {{ process_null('descricao_sub_atividade') }} as descricao_sub_atividade,
            {{ process_null('id_sub_atividade') }} as id_sub_atividade,
            {{ process_null('id_atividade') }} as id_atividade,
            {{ process_null('id_clinica') }} as id_clinica,
            {{ process_null('setor') }} as setor,
            {{ process_null('cid_evolucao') }} as cid_evolucao,
            {{ process_null('cid_descricao') }} as cid_descricao,
            {{ process_null('proc_evo') }} as proc_evo,
            {{ process_null('proc_descricao') }} as proc_descricao,
            cnes,
            loaded_at,
            cast(safe_cast(loaded_at as timestamp) as date) as data_particao
        from evolucao
        qualify row_number() over(partition by id_prontuario, id_boletim, cnes order by evolucao_data desc, loaded_at desc) = 1
    )

select *
from final