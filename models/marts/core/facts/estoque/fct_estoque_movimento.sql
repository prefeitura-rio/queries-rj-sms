{{
    config(
        alias="movimento",
        schema="saude_estoque",
        labels={"contains_pii": "no"},
        partition_by={
            "field": "data_particao",
            "data_type": "date",
            "granularity": "month",
        },
        enabled=false
    )
}}

--- sources
with
    source_vitai as (select * from {{ ref("raw_prontuario_vitai__estoque_movimento") }}),

    source_vitacare as (
        select
            est.*,
            est.material_quantidade * if(
                valor_unitario.material_valor_unitario_medio is null,
                0,
                valor_unitario.material_valor_unitario_medio
            ) as material_valor_total,
        from {{ ref("raw_prontuario_vitacare__estoque_movimento") }} as est
        left join
            {{ ref("int_estoque__material_valor_unitario_tpc") }} as valor_unitario
            on (est.id_material = valor_unitario.id_material)
    ),


--- transform into standard model
    movimento_vitai as (
        select
            est.id_cnes,
            est.id_material,
            est.material_descricao,
            est.material_unidade,
            est.estoque_secao_origem,
            est.estoque_secao_destino,
            est.estoque_movimento_tipo,
            est.estoque_movimento_justificativa,
            est.estoque_movimento_data,
            est.estoque_movimento_data_hora,
            est.material_quantidade,
            est.material_valor_total,
            est.data_particao,
            est.data_carga,
            case
                when
                    estoque_movimento_tipo = "TRANSFERENCIA ENTRADA"
                    or estoque_movimento_tipo = "TRANSFERENCIA SAIDA"
                then "Transferência Interna"
                when
                    estoque_movimento_tipo = "INVENTARIO ENTRADA"
                    or estoque_movimento_tipo = "INVENTARIO SAIDA"
                then "Ajuste de Inventário"
                when
                    estoque_movimento_tipo = "AJUSTE ENTRADA"
                    or estoque_movimento_tipo = "AJUSTE SAIDA"
                then "Ajuste na Entrada/Saída"
                when estoque_movimento_tipo = "PERDA"
                then "Avaria / Vencimento"
                else initcap(estoque_movimento_tipo)
            end as estoque_movimento_tipo_grupo,
            case
                when
                    estoque_movimento_tipo = "AJUSTE SAIDA"
                    or estoque_movimento_tipo = "CONSUMO"
                    or estoque_movimento_tipo = "INVENTARIO SAIDA"
                    or estoque_movimento_tipo = "PERDA"
                    or estoque_movimento_tipo = "SAIDA"
                    or estoque_movimento_tipo = "TRANSFERENCIA SAIDA"
                then - material_quantidade
                else material_quantidade
            end as material_quantidade_com_sinal,
            dispensacao_prescritor_cpf as estoque_movimento_consumo_prescritor_cpf,
            "" as estoque_movimento_consumo_prescritor_cns,
            dispensacao_paciente_cpf as estoque_movimento_consumo_paciente_cpf,
            dispensacao_paciente_cns as estoque_movimento_consumo_paciente_cns,
            "vitai" as sistema_origem
        from source_vitai as est
    ),

    
    movimento_vitacare as (
        select
            est.id_cnes,
            est.id_material,
            est.material_descricao,
            "" as material_unidade,
            "" as estoque_secao_origem,
            "" as estoque_secao_destino,
            est.estoque_movimento_tipo,
            est.estoque_movimento_justificativa,
            safe_cast(
                est.estoque_movimento_data_hora as date
            ) as estoque_movimento_data,
            est.estoque_movimento_data_hora,
            est.material_quantidade,
            est.material_valor_total,
            est.data_particao,
            est.data_carga,
            case
                when estoque_movimento_tipo = "NOVO LOTE"
                then "Entrada"
                when
                    estoque_movimento_tipo = "CORREÇÃO DE LOTE - AUMENTO"
                    or estoque_movimento_tipo = "CORREÇÃO DE LOTE - DIMINUIÇÃO"
                then "Ajuste de Inventário"
                when
                    estoque_movimento_tipo = "DISPENSA DE MEDICAMENTOS COM PRESCRIÇÃO"
                    or estoque_movimento_tipo
                    = "DISPENSA DE MEDICAMENTOS POR ADMINISTRAÇÃO"
                    or estoque_movimento_tipo = "DISPENSAÇÃO DE RECEITA EXTERNA"
                    or estoque_movimento_tipo
                    = "DISPENSAÇÃO DE RECEITA EXTERNA COM DATA ANTERIOR"
                    or estoque_movimento_tipo = "ANULAÇÃO DE DISPENSAS"
                then "Consumo"
                when
                    estoque_movimento_tipo = "REMOÇÃO DE LOTE"
                    or estoque_movimento_tipo = "SUSPENSÃO DE LOTE"
                    or estoque_movimento_tipo = "RECUPERAÇÃO DE LOTE"
                then "Avaria / Vencimento"
                else initcap(estoque_movimento_tipo)
            end as estoque_movimento_tipo_grupo,
            case
                when
                    estoque_movimento_tipo = "CORREÇÃO DE LOTE - DIMINUIÇÃO"
                    or estoque_movimento_tipo
                    = "DISPENSA DE MEDICAMENTOS COM PRESCRIÇÃO"
                    or estoque_movimento_tipo
                    = "DISPENSA DE MEDICAMENTOS POR ADMINISTRAÇÃO"
                    or estoque_movimento_tipo = "DISPENSAÇÃO DE RECEITA EXTERNA"
                    or estoque_movimento_tipo
                    = "DISPENSAÇÃO DE RECEITA EXTERNA COM DATA ANTERIOR"
                    or estoque_movimento_tipo = "REMOÇÃO DE LOTE"
                    or estoque_movimento_tipo = "SUSPENSÃO DE LOTE"
                then - material_quantidade
                else material_quantidade
            end as material_quantidade_com_sinal,
            "" as estoque_movimento_consumo_prescritor_cpf, 
            est.dispensacao_prescritor_cns
            as estoque_movimento_consumo_prescritor_cns,
            est.dispensacao_paciente_cpf as estoque_movimento_consumo_paciente_cpf,
            est.dispensacao_paciente_cns as estoque_movimento_consumo_paciente_cns,
            "vitacare" as sistema_origem,
        from source_vitacare as est
    ),

--- union

    movimento as (
        select *
        from movimento_vitai
        union all
        select *
        from movimento_vitacare
    )

select
    -- Primary Key
    -- Foreing Key
    id_cnes,
    id_material,

    -- Common Fields
    estoque_secao_origem as localizacao_origem,
    estoque_secao_destino as localizacao_destino,
    estoque_movimento_tipo as movimento_tipo,
    estoque_movimento_tipo_grupo as movimento_tipo_grupo,
    estoque_movimento_justificativa as movimento_justificativa,
    estoque_movimento_data as data_evento,
    estoque_movimento_data_hora as data_hora_evento,
    estoque_movimento_consumo_prescritor_cpf as consumo_prescritor_cpf,
    estoque_movimento_consumo_prescritor_cns as consumo_prescritor_cns,
    estoque_movimento_consumo_paciente_cns as consumo_paciente_cns,
    estoque_movimento_consumo_paciente_cpf as consumo_paciente_cpf,
    material_descricao,
    material_quantidade,
    material_quantidade_com_sinal,
    material_valor_total,
    if(
        material_quantidade_com_sinal < 0, - material_valor_total, material_valor_total
    ) as material_valor_total_com_sinal,

    -- Metadata
    sistema_origem,
    data_particao,
    data_carga

from movimento
