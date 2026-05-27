{{
    config(
        schema='brutos_prontuario_prontuaRio',
        alias="evolucao",
        materialized="incremental",
        incremental_strategy="merge",
        unique_key="id",
        tags=["prontuaRio"],
        partition_by={
            "field": "data_particao",
            "data_type": "date",
            "granularity": "day",
        },
    )
}}

{% set last_partition = get_last_partition_date(this) %}

with 

/*
  Ao executar o run/build com --full-refresh sem a lógica abaixo, é gerado um erro de uso de memória.
  Isso acontece devido ao conteúdo presente no campo `descricao` que contém extensos formulários em html.
  Para contornar este problema restringimos a execução para rodar apenas com dados extraídos no primeiro 
  dia de extração do ProntuaRio (20/05/2026).

  Os dados históricos (backup de novembro/2025) foram extraídos do dia 20/05/2026 até o dia 25/05/2026.
  Dessa forma, para processa-los é necessário executar uma materialização utilizando os parâmetros abaixo:

  $ dbt run -s raw_prontuario_prontuaRio__evolucao --vars '{"start_date": "2026-05-21", "end_date": "2026-05-21"}'

  A indicação é que seja feita dia a dia (até o dia 25/05/2026)
*/

    source_ as (
        select * from {{ source('brutos_prontuario_prontuaRio_staging', 'hp_rege_evolucao') }}
        {% if not is_incremental() %}
        /*

        */
          where date(loaded_at) between date('2026-05-20') and date('2026-05-20') -- Primeira extração do ProntuaRio
        {% endif %}
        {% if is_incremental() %} 
            where date(loaded_at) between date('{{ var("start_date", last_partition) }}')
                and date('{{ var("end_date", run_started_at.strftime("%Y-%m-%d")) }}')
        {% endif %}
    ),

    evolucao as (
        select
            json_extract_scalar(data, '$.id')               as id_prontuario,
            json_extract_scalar(data, '$.id_be')            as id_boletim,
            json_extract_scalar(data, '$.cns')              as cns,
            json_extract_scalar(data, '$.registro')         as registro, 
            json_extract_scalar(data, '$.data_reg')         as registro_data,
            json_extract_scalar(data, '$.data_evo')         as evolucao_data,
            json_extract_scalar(data, '$.profissional')     as nome_profissional,
            json_extract_scalar(data, '$.id_profissional')  as id_profissional,
            json_extract_scalar(data, '$.descricao')        as descricao_raw, -- processa depois
            json_extract_scalar(data, '$.tipo')             as tipo,
            json_extract_scalar(data, '$.data_atu')         as atualizacao_data,
            json_extract_scalar(data, '$.id_cen38')         as id_cen38,
            json_extract_scalar(data, '$.id_am12')          as id_am12,
            json_extract_scalar(data, '$.id_cen02')         as id_cen02,
            json_extract_scalar(data, '$.id_cen54')         as id_cen54,
            json_extract_scalar(data, '$.id_outro')         as id_outro,
            json_extract_scalar(data, '$.tip_outro')        as tip_outro,
            json_extract_scalar(data, '$.status_evol')      as status_evolucao,
            json_extract_scalar(data, '$.ds_sub_atividade') as descricao_sub_atividade,
            json_extract_scalar(data, '$.co_sub_atividade') as id_sub_atividade,
            json_extract_scalar(data, '$.co_atividade')     as id_atividade,
            json_extract_scalar(data, '$.codclin')          as id_clinica,
            json_extract_scalar(data, '$.setor')            as setor,
            json_extract_scalar(data, '$.cid_evo')          as cid_evolucao,
            json_extract_scalar(data, '$.cid_descricao')    as cid_descricao,
            json_extract_scalar(data, '$.proc_evo')         as proc_evo,
            json_extract_scalar(data, '$.proc_descricao')   as proc_descricao,
            cnes,
            loaded_at
        from source_
    ),

    final as (
        select
            safe_cast(id_prontuario as int64)   as id_prontuario, 
            safe_cast(id_boletim as int64)      as id_boletim,
            safe_cast(registro as int64)        as registro,
            {{ process_null('cns') }}           as cns,
            safe_cast(evolucao_data as datetime) as evolucao_data,
            safe_cast(registro_data as datetime) as registro_data,
            {{ process_null('nome_profissional') }} as nome_profissional,
            case 
                when id_profissional like '%000%' then cast(null as string)
                when id_profissional = '0'        then cast(null as string)
                else {{ process_null('id_profissional') }}
            end as cpf_profissional,
            {{ remove_html('descricao_raw') }}  as descricao, -- aplicado após dedup
            {{ process_null('tipo') }}          as tipo,
            safe_cast(atualizacao_data as datetime) as atualizacao_data,
            {{ process_null('id_cen38') }}      as id_cen38,
            {{ process_null('id_am12') }}       as id_am12,
            {{ process_null('id_cen02') }}      as id_cen02,
            {{ process_null('id_cen54') }}      as id_cen54,
            {{ process_null('id_outro') }}      as id_outro,
            {{ process_null('tip_outro') }}     as tip_outro,
            {{ process_null('status_evolucao') }} as status_evolucao,
            {{ process_null('descricao_sub_atividade') }} as descricao_sub_atividade,
            {{ process_null('id_sub_atividade') }} as id_sub_atividade,
            {{ process_null('id_atividade') }}  as id_atividade,
            {{ process_null('id_clinica') }}    as id_clinica,
            {{ process_null('setor') }}         as setor,
            {{ process_null('cid_evolucao') }}  as cid_evolucao,
            {{ process_null('cid_descricao') }} as cid_descricao,
            {{ process_null('proc_evo') }}      as proc_evo,
            {{ process_null('proc_descricao') }} as proc_descricao,
            cnes,
            loaded_at
        from evolucao
        {% if not is_incremental() %}
        qualify row_number() over(
            partition by id_prontuario, id_boletim, registro, cnes 
            order by loaded_at desc
        ) = 1
        {% endif %}
    )

select 
    {{
        dbt_utils.generate_surrogate_key(
            [
                'cnes',
                'id_prontuario',
                'id_boletim',
                'registro',
                'evolucao_data'
            ]
    )
    }} as id,
    concat(cnes, '.', id_prontuario) as gid_prontuario,
    concat(cnes, '.', id_boletim)   as gid_boletim,
    concat(cnes, '.', registro)     as gid_registro, 
    *,
    cast(loaded_at as date) as data_particao
from final