{{
    config(
        alias="vinculo_empregador",
        schema= "brutos_gdb_cnes",
        partition_by={
            "field": "data_particao",
            "data_type": "date",
            "granularity": "month",
        },
    )
}}

with source as (
    select
        json_extract_scalar(json, '$.CD_VINCULACAO') as CD_VINCULACAO,
        json_extract_scalar(json, '$.TP_VINCULO') as TP_VINCULO,
        json_extract_scalar(json, '$.DS_VINCULO') as DS_VINCULO,
        json_extract_scalar(json, '$.ST_HABILITADO') as ST_HABILITADO,
        _loaded_at,
        data_particao
    from {{ source("brutos_gdb_cnes_staging", "NFCES057") }}
),

renamed as (
    select
        cast({{ process_null("CD_VINCULACAO") }} as string) as id_vinculacao,
        cast({{ process_null("TP_VINCULO") }} as string) as id_tipo_vinculo,
        cast({{ process_null("DS_VINCULO") }} as string) as descricao_vinculo,
        case
            when lower(trim(ST_HABILITADO)) = 's' then true
            when lower(trim(ST_HABILITADO)) = 'n' then false
            else null
        end as habilitado,
        -- Podem ser usados posteriormente para deduplicação
        safe_cast(data_particao as date) as data_particao,
        safe_cast(_loaded_at as timestamp) as data_carga
    from source
)


select * from renamed