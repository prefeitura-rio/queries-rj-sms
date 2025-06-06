{{
    config(
        alias="paciente_cns",
        tags="smsrio"
    )
}}
with 
    source as (
        select * from {{ source('brutos_plataforma_smsrio_staging','sms_pacientes__tb_cns_provisorios') }}
    ),
    most_recent as (
        select * from source
        qualify row_number() over (partition by id order by timestamp desc) = 1
    ),
    transform as (
        select 
            -- Chave Primária
            SAFE_CAST(id as string) as id,
            
            -- Outras Chaves
            safe_cast(cns as string) as cns,
            safe_cast(cns_provisorio as string) as cns_provisorio,

            -- Metadata columns
            timestamp_add(datetime(timestamp({{process_null('timestamp')}}), 'America/Sao_Paulo'),interval 3 hour) as updated_at,
            datetime(timestamp(datalake_loaded_at), 'America/Sao_Paulo') as loaded_at
        from most_recent
    )
select * from source