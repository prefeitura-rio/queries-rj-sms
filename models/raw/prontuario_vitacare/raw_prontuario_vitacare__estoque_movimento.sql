{{
    config(
        alias="estoque_movimento",
    )
}}

with
    source as (
        select *
        from {{ source("brutos_prontuario_vitacare_staging", "estoque_movimento") }}
    ),
    final as (
        select
            safe_cast(ap as string) as area_programatica,
            safe_cast(cnesunidade as string) as id_cnes,
            safe_cast(nomeunidade as string) as estabelecimento_nome,
            safe_cast(desigmedicamento as string) as material_descricao,
            safe_cast(atc as string) as id_atc,
            regexp_replace(
                safe_cast(code as string), r'[^a-zA-Z0-9]', ''
            ) as id_material,
            safe_cast(lote as string) as id_lote,
            safe_cast(dtamovimento as datetime) as estoque_movimento_data_hora,
            safe_cast(tipomovimento as string) as estoque_movimento_tipo,
            safe_cast(motivocorrecao as string) as estoque_movimento_correcao_tipo,
            safe_cast(justificativa as string) as estoque_movimento_justificativa,
            safe_cast(cpfprofprescritor as string) as dispensacao_prescritor_cpf,
            safe_cast(cnsprofprescritor as string) as dispensacao_prescritor_cns,
            safe_cast(cpfpatient as string) as dispensacao_paciente_cpf,
            safe_cast(cnspatient as string) as dispensacao_paciente_cns,
            safe_cast(qtd as float64) as material_quantidade,
            safe_cast(codwms as string) as id_pedido_wms,
            safe_cast(armazemorigem as string) as estoque_armazem_origem,
            safe_cast(armazemdestino as string) as estoque_armazem_destino,
            safe_cast(id as string) as id_estoque_movimento,
            safe_cast(_data_carga as datetime) as data_carga,
            safe_cast(ano_particao as int) as ano_particao,
            safe_cast(mes_particao as int) as mes_particao,
            safe_cast(data_particao as date) as data_particao

        from source
    )
select
    -- Primary Key
    id_estoque_movimento,

    -- Foreign Key
    area_programatica,
    id_cnes,
    id_pedido_wms,
    id_lote,
    id_material,
    id_atc,

-- Common Fields
    estabelecimento_nome,
    material_descricao,
    estoque_movimento_data_hora,
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


-- Metada
    data_particao,
    data_carga,

from final
