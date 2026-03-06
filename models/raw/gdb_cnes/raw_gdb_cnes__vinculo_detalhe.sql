{{
    config(
        alias="vinculo_detalhe",
        schema= "brutos_gdb_cnes",
        partition_by={
            "field": "data_particao",
            "data_type": "date",
            "granularity": "month",
        },
    )
}}


with 
source as (
    select
        json_extract_scalar(json, '$.IND_VINC') as IND_VINC,
        json_extract_scalar(json, '$.CD_VINCULACAO') as CD_VINCULACAO,
        json_extract_scalar(json, '$.TP_VINCULO') as TP_VINCULO,
        json_extract_scalar(json, '$.TP_SUBVINCULO') as TP_SUBVINCULO,
        json_extract_scalar(json, '$.DS_SUBVINCULO') as DS_SUBVINCULO,
        json_extract_scalar(json, '$.DS_CONCEITO') as DS_CONCEITO,
        json_extract_scalar(json, '$.ST_HABILITADO') as ST_HABILITADO,
        json_extract_scalar(json, '$.ST_SOLICITA_CNPJ') as ST_SOLICITA_CNPJ,
        _loaded_at,
        data_particao
    from {{ source("brutos_gdb_cnes_staging", "NFCES058") }}
),

renamed as (
    select
        cast({{ process_null("IND_VINC") }} as string) as id_vinculo,
        -- CD_VINCULACAO: FK NFCES057
        cast({{ process_null("CD_VINCULACAO") }} as string) as id_vinculacao,
        cast({{ process_null("TP_VINCULO") }} as string) as tipo_vinculo,
        cast({{ process_null("TP_SUBVINCULO") }} as string) as tipo_subvinculo,
        cast({{ process_null("DS_SUBVINCULO") }} as string) as descricao_subvinculo,
        cast({{ process_null("DS_CONCEITO") }} as string) as descricao_conceito,
        case
            when lower(trim(ST_HABILITADO)) = 's' then true
            when lower(trim(ST_HABILITADO)) = 'n' then false
            else null
        end as habilitado,
        case
            when lower(trim(ST_SOLICITA_CNPJ)) = 's' then true
            when lower(trim(ST_SOLICITA_CNPJ)) = 'n' then false
            else null
        end as solicita_cnpj,
        
        -- Podem ser usados posteriormente para deduplicação
        safe_cast(data_particao as date) as data_particao,
        safe_cast(_loaded_at as timestamp) as data_carga
    from source
)
select * from renamed
