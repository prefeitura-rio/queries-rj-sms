{{
    config(
        alias="numeros_processamento",
        schema= "brutos_gdb_sih"
    )
}}

with source as (
    select * from {{ source('brutos_gdb_sih_staging', 'TB_PR') }}
),
extracted as (
    select

        json_extract_scalar(json, "$.PR_CMPT") as PR_CMPT,
        json_extract_scalar(json, "$.PR_NUM") as PR_NUM,
        json_extract_scalar(json, "$.PR_DESCRICAO") as PR_DESCRICAO,
        json_extract_scalar(json, "$.PR_VINC_PG_PF") as PR_VINC_PG_PF,
        json_extract_scalar(json, "$.PR_VINC_PG_CNPJ") as PR_VINC_PG_CNPJ,
        json_extract_scalar(json, "$.PR_VINC_PG_CNES") as PR_VINC_PG_CNES,
        json_extract_scalar(json, "$.PR_CONCLUIDO") as PR_CONCLUIDO,
        json_extract_scalar(json, "$.PR_SITUACAO") as PR_SITUACAO,
        json_extract_scalar(json, "$.PR_HISTORICO_DATA") as PR_HISTORICO_DATA,
        json_extract_scalar(json, "$.PR_APAGA_DATA") as PR_APAGA_DATA,
        json_extract_scalar(json, "$.PR_ELETIVAS") as PR_ELETIVAS,

        _source_file,
        _loaded_at,
        data_particao
    from source
),

renamed as (
    select

        cast({{ process_null("PR_CMPT") }} as string) as cmpt,
        cast({{ process_null("PR_NUM") }} as string) as numero,
        cast({{ process_null("trim(PR_DESCRICAO)") }} as string) as descricao,
        cast({{ process_null("PR_VINC_PG_PF") }} as string) as vinculo_pg_pf,
        cast({{ process_null("PR_VINC_PG_CNPJ") }} as string) as vinculo_pg_cnpj,
        cast({{ process_null("PR_VINC_PG_CNES") }} as string) as vinculo_pg_cnes,
        cast({{ process_null("PR_CONCLUIDO") }} as string) as concluido,
        cast({{ process_null("PR_SITUACAO") }} as string) as situacao,
        cast({{ process_null("PR_HISTORICO_DATA") }} as string) as historico_data,
        cast({{ process_null("PR_APAGA_DATA") }} as string) as apaga_data,
        cast({{ process_null("PR_ELETIVAS") }} as string) as eletivas,

        -- Podem ser usados posteriormente para deduplicação
        data_particao,
        _loaded_at as data_carga,
    from extracted
)
select *
from renamed
