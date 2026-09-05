{{
    config(
        alias="fat_exame_item",
        materialized="table",
        schema="intermediario_prontuario_vitai_dtw",
        tags=["intermediate", "vitai"]
    )
}}

with

    raw_exame_item as (
        select *
        from {{ ref("raw_prontuario_vitai_dtw_fat_exame_item") }}
    ),

    dim_procedimento as (
        select prc_id, prc_codigo, prc_descricao
        from {{ ref("raw_prontuario_vitai_dtw_dim_procedimento") }}
    ),

    final as (
        select
            -- Chave primária
            ei.exi_id,

            -- Chaves estrangeiras
            ei.exame_id,
            ei.prc_id,
            ei.sie_id,

            -- Dados do item de exame
            nullif(trim(ei.exame_gid), '') as exame_gid,
            nullif(trim(ei.exame_mneumonico), '') as exame_mneumonico,
            nullif(trim(ei.exame_descricao), '') as exame_descricao,
            ei.codigo_interno,

            -- Descrições dimensionais
            prc.prc_codigo,
            prc.prc_descricao,

            -- Datas
            ei.data_realizacao,
            ei.data_liberacao,
            ei.data_exclusao,

            -- Metadados
            ei.created_at,
            ei.updated_at,
            ei.loaded_at,
            ei.data_particao
        from raw_exame_item as ei
        left join dim_procedimento as prc on ei.prc_id = prc.prc_id
        where ei.exi_id is not null
    )

select *
from final
