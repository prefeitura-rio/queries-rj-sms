{{
    config(
        alias="fat_item_prescricao",
        materialized="table",
        schema="intermediario_prontuario_vitai_dtw",
        tags=["intermediate", "vitai"]
    )
}}

with

    raw_item_prescricao as (
        select *
        from {{ ref("raw_prontuario_vitai_dtw_fat_item_prescricao") }}
    ),

    dim_tipo_item_prescricao as (
        select tip_id, tip_descricao
        from {{ ref("raw_prontuario_vitai_dtw_dim_tipo_item_prescricao") }}
    ),

    dim_via_administracao as (
        select via_id, via_descricao
        from {{ ref("raw_prontuario_vitai_dtw_dim_via_administracao") }}
    ),

    dim_unidade_medida as (
        select unm_id, unm_descricao
        from {{ ref("raw_prontuario_vitai_dtw_dim_unidade_medida") }}
    ),

    final as (
        select
            -- Chave primária
            ip.item_prescricao_gid,

            -- Chaves estrangeiras
            ip.prescricao_gid,
            ip.boletim_gid,
            ip.estabelecimento_gid,
            ip.tip_id,
            ip.via_id,
            ip.unm_id,

            -- Dados do item
            nullif(trim(ip.item_prescrito), '') as item_prescrito,
            nullif(trim(ip.pri_descricaoitem), '') as pri_descricaoitem,
            nullif(trim(ip.produto_associado), '') as produto_associado,
            nullif(trim(ip.orientacao_uso), '') as orientacao_uso,
            nullif(trim(ip.observacao), '') as observacao,
            upper(trim(ip.is_antibiotico)) as is_antibiotico,
            ip.quantidade,

            -- Descrições dimensionais
            tip.tip_descricao,
            via.via_descricao,
            unm.unm_descricao,

            -- Datas
            ip.datahora_cadastro,

            -- Metadados
            ip.created_at,
            ip.updated_at,
            ip.loaded_at,
            ip.data_particao
        from raw_item_prescricao as ip
        left join dim_tipo_item_prescricao as tip on ip.tip_id = tip.tip_id
        left join dim_via_administracao as via on ip.via_id = via.via_id
        left join dim_unidade_medida as unm on ip.unm_id = unm.unm_id
        where ip.item_prescricao_gid is not null
    )

select *
from final
