{{
    config(
        alias="fat_profissional",
        materialized="table",
        schema="intermediario_prontuario_vitai_dtw",
        tags=["intermediate", "vitai"]
    )
}}

with

    raw_profissional as (
        select *
        from {{ ref("raw_prontuario_vitai_dtw_fat_profissional") }}
    ),

    final as (
        select
            -- Chave primária
            prf_id,

            -- Identificadores sensíveis
            nullif(trim(cpf), '') as cpf,
            nullif(trim(cns), '') as cns,

            -- Registro profissional
            cre_id,
            nullif(trim(numero_conselho), '') as numero_conselho,
            upper(trim(uf_conselho)) as uf_conselho,

            -- Dados pessoais
            initcap(trim(nome)) as nome,

            -- Status
            upper(trim(situacao)) as situacao,

            -- Metadados
            datahora,
            created_at,
            updated_at,
            loaded_at,
            data_particao
        from raw_profissional
        where prf_id is not null
    )

select *
from final
