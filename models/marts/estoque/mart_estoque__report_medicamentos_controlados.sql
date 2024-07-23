{{
    config(
        alias="rp_medicamentos_controlados",
        schema="projeto_estoque",
        materialized="table",
    )
}}


with
    -- SOURCES
    movimento as (
        select *
        from {{ ref("fct_estoque_movimento") }}
        where
            sistema_origem = 'vitacare'
            and material_quantidade <> 0
            and data_particao
            >= date_sub(current_date('America/Sao_Paulo'), interval 6 day)
            and id_cnes in ("2280787", "7523246", "2288370")  -- Nilza Rosa (22), Nelio de
    -- Oliveira (10), Pindaro de Carvalho (21)
    ),

    posicao as (
        select
            id_cnes,
            id_material,
            data_particao,
            sum(material_quantidade) as posicao_quantidade
        from {{ ref("fct_estoque_posicao") }}
        where
            data_particao = date_sub(current_date('America/Sao_Paulo'), interval 6 day)
        group by 1, 2, 3
    ),

    validade as (
        select id_cnes, id_material, id_lote, max(lote_data_vencimento) as data_validade
        from {{ ref("fct_estoque_posicao") }}
        group by 1, 2, 3
    ),

    material as (select * from {{ ref("dim_material") }}),

    movimento_controlados as (
        select mov.*, mat.controlado_tipo, mat.nome
        from movimento as mov
        left join material as mat using (id_material)
        where controlado_indicador = 'sim'
    ),

    -- TRASNFORMATIONS
    eventos as (
        select
            id_cnes,
            id_material,
            nome,
            controlado_tipo,
            id_lote,
            data_evento,
            movimento_entrada_saida,
            movimento_tipo_grupo,
            movimento_tipo,
            movimento_justificativa,
            id_pedido_wms,
            consumo_paciente_cpf,
            material_quantidade_com_sinal as quantidade,
            if(
                movimento_entrada_saida = 'SAIDA'
                and movimento_tipo_grupo = 'AVARIA / VENCIMENTO',
                "perda",
                lower(movimento_entrada_saida)
            ) as tipo_evento,
            case
                when movimento_tipo_grupo = "ENTRADA DE ESTOQUE"
                then concat("Pedido WMS: ", coalesce(id_pedido_wms, "não registrado"))
                when movimento_tipo_grupo = "TRANSFERENCIA EXTERNA"
                then "Transferência entre unidades"
                when movimento_tipo_grupo = "CORRECAO DE ESTOQUE / OUTRO"
                then "Correção de lote (aumento) / Outro"
                when
                    movimento_tipo_grupo = "CONSUMO"
                    and movimento_tipo = "ATENDIMENTO EXTERNO"
                then "ATENDIMENTO EXTERNO"
                when
                    movimento_tipo_grupo = "CONSUMO"
                    and movimento_tipo != "ATENDIMENTO EXTERNO"
                then
                    concat(
                        "Consumo paciente: ",
                        coalesce(
                            concat(
                                substr(consumo_paciente_cpf, 1, 3),
                                '.',
                                substr(consumo_paciente_cpf, 4, 3),
                                '.',
                                substr(consumo_paciente_cpf, 7, 3),
                                '-',
                                substr(consumo_paciente_cpf, 10, 2)
                            ),
                            "não registrado"
                        )
                    )
                when movimento_tipo_grupo = "TRANSFERENCIA EXTERNA"
                then "Transferência entre unidades"
                when movimento_tipo_grupo = "CORRECAO DE ESTOQUE / OUTRO"
                then "Correção de lote (diminuição) / Outro"
                when movimento_tipo = "AVARIA"
                then "Avaria"
                when movimento_tipo = "VALIDADE_EXPIRADA"
                then "Validade expirada"
                else ""
            end as evento

        from movimento_controlados
    ),

    eventos_ordenados as (
        select
            *,
            row_number() over (
                partition by id_cnes, id_material order by data_evento, tipo_evento
            ) as ordem
        from eventos
        order by id_cnes, nome, data_evento, tipo_evento
    ),

    eventos_final as (
        select
            mov.id_cnes,
            mov.id_material,
            mov.nome,
            mov.controlado_tipo,
            mov.id_lote,
            val.data_validade,
            mov.tipo_evento,
            mov.evento,
            mov.data_evento,
            mov.ordem,
            mov.quantidade as movimento_quantidade,
            sum(mov.quantidade) over (
                partition by mov.id_cnes, mov.id_material
                order by mov.ordem
                rows between unbounded preceding and current row
            ) as movimento_quantidade_acumulada
        from eventos_ordenados as mov
        left join validade as val using (id_cnes, id_material, id_lote)
        order by id_cnes, id_material, ordem
    ),

    -- FINAL TABLE
    final as (
        select
            eventos.*,
            coalesce(posicao_quantidade, 0) as posicao_inicial,
            (
                coalesce(posicao_quantidade, 0) + movimento_quantidade_acumulada
            ) as posicao_final,
        from eventos_final as eventos
        left join posicao using (id_cnes, id_material)
    )

select
    id_cnes,
    id_material,
    nome,
    controlado_tipo,
    id_lote,
    data_validade,
    tipo_evento,
    evento,
    data_evento,
    ordem,
    --posicao_inicial,
    movimento_quantidade,
    --movimento_quantidade_acumulada,
    posicao_final
from final
