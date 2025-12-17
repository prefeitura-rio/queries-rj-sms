{{
    config(
        alias="profissional",
        schema= "brutos_gdb_cnes",
        materialized="table",
        partition_by={
            "field": "cpf_particao",
            "data_type": "int64",
            "range": {"start": 0, "end": 100000000000, "interval": 34722222},
        }
    )
}}

with source as (
    select * from {{ source('brutos_gdb_cnes_staging', 'LFCES018') }}
),
extracted as (
    select
        json_extract_scalar(json, "$.PROF_ID") as PROF_ID,
        json_extract_scalar(json, "$.CPF_PROF") as CPF_PROF,
        json_extract_scalar(json, "$.COD_CNS") as COD_CNS,
        json_extract_scalar(json, "$.NOME_PROF") as NOME_PROF,
        json_extract_scalar(json, "$.DATA_NASC") as DATA_NASC,
        json_extract_scalar(json, "$.SEXO") as SEXO,
        _source_file,
        _loaded_at,
        data_particao
    from source
),

renamed as (
    select
        substr(upper(to_hex(md5(cast(PROF_ID as string)))), 0, 16) as id_profissional_sus,

        cast({{process_null('PROF_ID')}} as string) as id_profissional_cnes,
        cast({{process_null('CPF_PROF')}} as string) as cpf,
        cast({{process_null('COD_CNS')}} as string) as cns,
        cast({{process_null('NOME_PROF')}} as string) as nome,
        safe_cast({{process_null('DATA_NASC')}} as date) as data_nascimento,
        case
            when lower(trim(SEXO))='f' then 'Feminino'
            when lower(trim(SEXO))='m' then 'Masculino'
            -- único outro valor aqui é strig vazia
            else null
        end as sexo,

        -- Podem ser usados posteriormente para deduplicação
        data_particao,
        _loaded_at as data_carga,

        -- Precisamos usar safe_cast() porque aparece um (01) CPF com letra no meio
        safe_cast({{process_null("CPF_PROF")}} as int64) as cpf_particao
    from extracted
)
select *
from renamed
