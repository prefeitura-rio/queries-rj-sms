with
    -- sources
    estoque_posicao_formato_novo as (
        select *
        from {{ ref("raw_prontuario_vitacare_api__estoque_posicao") }}
    ),

    -- transformations
    final as (
        select
            id,
            id_surrogate,
            area_programatica,
            id_cnes,
            id_lote,
            id_material,
            id_atc,
            estabelecimento_nome,
            lote_status,
            lote_data_cadastro,
            lote_data_vencimento,
            material_descricao,
            material_quantidade,
            armazem,
            cast(metadados.loaded_at as date) as data_particao,
            cast(metadados.updated_at as datetime) as data_replicacao,
            cast(metadados.loaded_at as date) as data_carga
        from estoque_posicao_formato_novo
        where material_quantidade > 0
    )

select *
from final
