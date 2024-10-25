{{
    config(
        alias="ficha_ponto",
        materialized="table",
        unique_key="id",
    )
}}

with
    source as (
        select * from {{ source("brutos_seguir_em_frente_staging", "ficha_ponto") }}
    ),
    renamed as (
        select
            {{ adapter.quote("id") }},
            {{ adapter.quote("title") }},
            {{ adapter.quote("current_phase") }},
            {{ adapter.quote("creator") }} as criado_por,
            {{ adapter.quote("created_at") }} as criado_em,
            {{ adapter.quote("bolsista") }} as id_nome_cpf,
            {{ adapter.quote("mes") }} as competencia,
            {{ adapter.quote("folhas_de_ponto") }} as url,
            {{ adapter.quote("ano_particao") }},
            {{ adapter.quote("mes_particao") }},
            {{ adapter.quote("data_particao") }}

        from source
    )
select
    -- pk
    id,

    -- fk
    id_nome_cpf,
    {{clean_numeric_string("split(id_nome_cpf, '_')[1]")}} as cpf,

    -- fields
    competencia,
    split(url, ",") as url,

    -- metadata
    criado_por,
    criado_em,
    ano_particao,
    mes_particao,
    safe_cast(data_particao as date) as data_particao
from renamed
