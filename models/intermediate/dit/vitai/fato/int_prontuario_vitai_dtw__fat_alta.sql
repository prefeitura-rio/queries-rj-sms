{{
    config(
        alias="fat_alta",
        materialized="table",
        schema="intermediario_prontuario_vitai_dtw",
        tags=["intermediate", "vitai"]
    )
}}

with

    raw_alta as (
        select *
        from {{ ref("raw_prontuario_vitai_dtw_fat_alta") }}
    ),

    dim_motivo_alta as (
        select mal_id, mal_descricao
        from {{ ref("raw_prontuario_vitai_dtw_dim_motivo_alta") }}
    ),

    final as (
        select
            -- Chave primária
            a.alta_gid,

            -- Chaves estrangeiras
            a.boletim_gid,
            a.estabelecimento_gid,
            a.mal_id,

            -- Dados da alta
            upper(trim(a.status)) as status,
            nullif(trim(a.tipo_alta_detalhada), '') as tipo_alta_detalhada,
            nullif(trim(a.motivo_saida), '') as motivo_saida,
            nullif(trim(a.abe_obs), '') as abe_obs,

            -- Descrições dimensionais
            mal.mal_descricao,

            -- Datas
            a.data_alta,
            a.alta_medica,
            a.alta_administrativa,
            a.data_obito,

            -- Metadados
            a.created_at,
            a.updated_at,
            a.loaded_at,
            a.data_particao
        from raw_alta as a
        left join dim_motivo_alta as mal on a.mal_id = mal.mal_id
        where a.alta_gid is not null
    )

select *
from final
