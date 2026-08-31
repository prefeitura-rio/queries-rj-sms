{{
    config(
        schema="intermediario_gdb_cnes",
        alias="profissional",
        materialized="table",
        tags=["gdb_cnes"],
        partition_by={
            "field": "cpf_particao",
            "data_type": "int64",
            "range": {"start": 0, "end": 100000000000, "interval": 34722222},
        }
    )
}}


with
    profissional as (
        select
            substr(upper(to_hex(md5(cast(PROF_ID as string)))), 0, 16) as id_profissional_sus,

            cast({{ process_null("PROF_ID") }} as string) as id_profissional_cnes,
            cast({{ process_null("CPF_PROF") }} as string) as cpf,
            cast({{ process_null("COD_CNS") }} as string) as cns,
            cast({{ process_null("NOME_PROF") }} as string) as nome,
            safe_cast({{ process_null("DATA_NASC") }} as date) as data_nascimento,
            case
                when lower(trim(SEXO))='f' then 'Feminino'
                when lower(trim(SEXO))='m' then 'Masculino'
                -- único outro valor aqui é string vazia
                else null
            end as sexo,

            data_particao as competencia,
            _loaded_at,

            -- Precisamos usar safe_cast() porque aparece um (01) CPF com letra no meio
            safe_cast({{ process_null("CPF_PROF") }} as int64) as cpf_particao

        from {{ ref("raw_gdb_cnes__lfces018") }}
        where data_particao = (
            select max(data_particao)
            from {{ ref("raw_gdb_cnes__lfces018") }}
        )
        qualify row_number() over (
            partition by id_profissional_cnes
            order by _loaded_at desc
        ) = 1
    )

select *
from profissional
