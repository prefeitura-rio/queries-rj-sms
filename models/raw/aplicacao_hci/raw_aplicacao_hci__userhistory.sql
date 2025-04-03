{{
    config(
        alias="usuario_historico",
        tags="aplicacao_hci"
    )
}}
with 
    source as (
        select * from {{ source('brutos_aplicacao_hci_staging','public__userhistory') }}
    ),
    most_recent as (
        select * from source
        qualify row_number() over (partition by id order by timestamp desc) = 1
    ),
    casted as (
        select
            id,
            method as metodo,
            path,
            split(path, '/')[safe_offset(3)] as entidade_consultada,
            split(path, '/')[safe_offset(4)] as cpf_consultado,
            safe_cast(status_code as int64) as codigo_status,
            safe_cast(user_id as int64) as id_usuario,
            safe_cast(timestamp as timestamp) as updated_at,
            safe_cast(datalake_loaded_at as timestamp) as loaded_at
        from most_recent
    )
select * from casted