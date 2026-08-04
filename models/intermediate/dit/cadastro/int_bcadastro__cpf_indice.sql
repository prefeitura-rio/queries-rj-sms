{{
    config(
        alias="bcadastro_cpf_indice",
        materialized="table",
        partition_by={
            "field": "hash_particao",
            "data_type": "int64",
            "range": {"start": 0, "end": 1024, "interval": 1},
        },
        cluster_by=["hash_match"],
        tags=["weekly"]
    )
}}

with
    normalizado as (
        select
            {{ clean_numeric("cpf") }} as cpf,
            nullif(
                {{ remove_duplicate_whitespace(remove_accents_upper("nome")) }},
                ""
            ) as nome_normalizado,
            nascimento_data as data_nascimento,
            nullif(
                {{ remove_duplicate_whitespace(remove_accents_upper("mae_nome")) }},
                ""
            ) as mae_nome_normalizado
        from {{ ref("raw_bcadastro__cpf") }}
    ),

    com_primeiro_nome as (
        select
            *,
            split(nome_normalizado, " ")[safe_offset(0)] as primeiro_nome_normalizado
        from normalizado
        where
            {{ validate_cpf("cpf") }}
            and nome_normalizado is not null
            and data_nascimento is not null
            and mae_nome_normalizado is not null
    ),

    com_hashes as (
        select
            cpf,
            nome_normalizado,
            primeiro_nome_normalizado,
            data_nascimento,
            mae_nome_normalizado,
            mod(abs(farm_fingerprint(primeiro_nome_normalizado)), 1024) as hash_particao,
            farm_fingerprint(
                concat(
                    nome_normalizado,
                    "|",
                    format_date("%F", data_nascimento),
                    "|",
                    mae_nome_normalizado
                )
            ) as hash_match
        from com_primeiro_nome
    )

select distinct *
from com_hashes
