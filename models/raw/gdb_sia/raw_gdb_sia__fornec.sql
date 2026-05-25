{{
    config(
        alias="fornec",
        schema= "brutos_gdb_sia",
        partition_by={
            "field": "data_particao",
            "data_type": "date",
            "granularity": "month",
        },
    )
}}

with source as (
    select * from {{ source('brutos_gdb_sia_staging', 'TB_FORNEC') }}
),
extracted as (
    select

        json_extract_scalar(json, "$.FOR_CMP") as FOR_CMP,
        json_extract_scalar(json, "$.FOR_GESTOR") as FOR_GESTOR,
        json_extract_scalar(json, "$.FOR_CONDIC") as FOR_CONDIC,
        json_extract_scalar(json, "$.FOR_RZSOC") as FOR_RZSOC,
        json_extract_scalar(json, "$.FOR_FANTA") as FOR_FANTA,
        json_extract_scalar(json, "$.FOR_CNPJ") as FOR_CNPJ,
        json_extract_scalar(json, "$.FOR_BANCO") as FOR_BANCO,
        json_extract_scalar(json, "$.FOR_AG") as FOR_AG,
        json_extract_scalar(json, "$.FOR_CC") as FOR_CC,
        json_extract_scalar(json, "$.FOR_END") as FOR_END,
        json_extract_scalar(json, "$.FOR_BAIRRO") as FOR_BAIRRO,
        json_extract_scalar(json, "$.FOR_CEP") as FOR_CEP,
        json_extract_scalar(json, "$.FOR_IBGE") as FOR_IBGE,
        -- json_extract_scalar(json, "$.CHECK_SUM") as CHECK_SUM

        _source_file,
        _loaded_at,
        data_particao
    from source
),

renamed as (
    select
        case
            when length(trim(FOR_CMP)) = 6 and regexp_contains(trim(FOR_CMP), r'^[0-9]+$')
                then concat(left(trim(FOR_CMP), 4), '-', right(trim(FOR_CMP), 2))
            else cast({{ process_null("FOR_CMP") }} as string)
        end as mes_competencia,
        cast({{ process_null("FOR_GESTOR") }} as string) as gestor,
        cast({{ process_null("FOR_CONDIC") }} as string) as condic,

        -- Pessoa jurídica
        cast({{ process_null("trim(FOR_RZSOC)") }} as string) as razao_social,
        cast({{ process_null("trim(FOR_FANTA)") }} as string) as nome_fantasia,
        case
            when REGEXP_CONTAINS(trim(FOR_CNPJ), r"^0+$")
                then null
            else cast({{ process_null("FOR_CNPJ") }} as string)
        end as cnpj,

        -- Banco
        cast({{ process_null("trim(FOR_BANCO)") }} as string) as banco,
        case
            when REGEXP_CONTAINS(trim(FOR_AG), r"^0+$")
                then null
            else cast({{ process_null("FOR_AG") }} as string)
        end as agencia,
        case
            when REGEXP_CONTAINS(trim(FOR_CC), r"^0+$")
                then null
            else cast({{ process_null("FOR_CC") }} as string)
        end as conta,

        -- Endereço
        cast({{ process_null("trim(FOR_END)") }} as string) as endereco,
        cast({{ process_null("trim(FOR_BAIRRO)") }} as string) as bairro,
        case
            when REGEXP_CONTAINS(trim(FOR_CEP), r"^0+$")
                then null
            else cast({{ process_null("FOR_CEP") }} as string)
        end as cep,
        case
            when REGEXP_CONTAINS(trim(FOR_IBGE), r"^0+$")
                then null
            else cast({{ process_null("FOR_IBGE") }} as string)
        end as ibge,

        -- Podem ser usados posteriormente para deduplicação
        safe_cast(data_particao as date) as data_particao,
        safe_cast(_loaded_at as timestamp) as data_carga
    from extracted
)
select *
from renamed
