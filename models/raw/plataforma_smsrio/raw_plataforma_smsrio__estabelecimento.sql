{{
    config(
        alias="estabelecimento",
    )
}}

with
    source as (
        select * from {{ source('brutos_plataforma_smsrio_staging','subpav_cnes__unidades') }}
    ),
    most_recent as (
        select * from source
        qualify row_number() over (partition by id order by updated_at desc) = 1
    ),
    renamed as (
        select
            cast(id as int64) as id,
            format("%07d", cast(cnes as int64)) as id_cnes,
            {{process_null('ap')}} as area_programtica,
            {{process_null('nome_fanta')}} as nome_fantasia,
            {{process_null('r_social')}} as razao_social,
            safe_cast(dt_atualiza as date) as data_atualizacao,
            {{process_null('tp_gestao')}} as tipo_gestao,
            if(tp_estab_sempre_aberto = '1', true, false) as sempre_aberto_indicador,
            safe_cast(dt_inaugura as date) as data_inauguracao,
            {{process_null('tipo_unidade_id')}} as tipo_unidade_id,
            cod_turnat_id,
            {{process_null('motivo_desativacao_unidade_id')}} as motivo_desativacao_unidade_id,
            {{process_null('natureza_juridica_id')}} as natureza_juridica_id,
            {{process_null('tipo_estabelecimento_id')}} as tipo_estabelecimento_id,
            {{process_null('atividade_principal_id')}} as atividade_principal_id,
            {{process_null('prof_diretor_id') }} as prof_diretor_id,
            safe_cast(datalake_loaded_at as timestamp) as datalake_loaded_at,
            timestamp_add(datetime(timestamp({{process_null('created_at')}}), 'America/Sao_Paulo'),interval 3 hour) as created_at,
            timestamp_add(datetime(timestamp({{process_null('updated_at')}}), 'America/Sao_Paulo'),interval 3 hour) as updated_at,
        from most_recent
    )
select *
from renamed
