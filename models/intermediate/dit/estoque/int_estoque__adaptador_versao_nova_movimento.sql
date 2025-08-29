with
    -- sources
    estoque_posicao_formato_novo as (
        select * from {{ ref("raw_prontuario_vitacare__estoque_movimento") }}
    ),

    -- transformations
    final as (
        select 
            -- Primary Key
            id_estoque_movimento,
            id_surrogate,

            -- Foreign Key
            area_programatica,
            id_cnes,
            id_pedido_wms,
            id_material,
            id_atc,

            -- Common Fields
            estabelecimento_nome,
            material_descricao,
            estoque_movimento_data_hora,
            id_lote,
            estoque_movimento_tipo,
            estoque_movimento_correcao_tipo,
            estoque_movimento_justificativa,
            estoque_armazem_origem,
            estoque_armazem_destino,
            dispensacao_prescritor_cpf,
            dispensacao_prescritor_cns,
            dispensacao_paciente_cpf,
            dispensacao_paciente_cns,
            material_quantidade,

            -- Metadata
            particao_data_movimento as data_particao,
            safe_cast(metadados.loaded_at as datetime) as data_carga,
        from estoque_posicao_formato_novo
    )

select * from final