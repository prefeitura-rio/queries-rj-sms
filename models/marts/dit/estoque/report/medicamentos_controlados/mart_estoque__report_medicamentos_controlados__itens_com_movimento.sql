{{
    config(
        enabled= false,
        alias="report_medicamentos_controlados__itens_com_movimento",
        schema="projeto_estoque",
        materialized="table",
        tag=["report", "weekly"],
    )
}}


with
    posicao_inicial_por_estabelecimento as (
        select *
        from
            {{ ref('int_estoque__report_controlados__primeira_posicao_por_estabelecimento') }}
    ),

    menor_data_posicao as (
        select min(data_particao) as menor_data
        from posicao_inicial_por_estabelecimento
    ),

    primeiro_dia_mes as (
        select date_trunc(current_date('America/Sao_Paulo'), month) as data
    ),

    movimento as (
        select mov.*

        from {{ ref("fct_estoque_movimento") }} as mov

        inner join posicao_inicial_por_estabelecimento as pos
        on mov.id_cnes = pos.id_cnes and mov.data_particao >= pos.data_particao

        where
            mov.sistema_origem = 'vitacare'
            and mov.material_quantidade <> 0

    ),

    posicao as (
        select
            id_cnes,
            id_material,
            data_particao,
            sum(material_quantidade) as posicao_quantidade
        from {{ ref("fct_estoque_posicao") }}
        inner join posicao_inicial_por_estabelecimento using (id_cnes, data_particao)
        group by 1, 2, 3
    ),

    controlados as (
        select * from {{ ref("dim_material") }} where controlado_indicador = 'sim'
    ),

    estabelecimento as (select * from {{ ref("dim_estabelecimento") }}),

    validade as (
        select id_cnes, id_material, id_lote, max(lote_data_vencimento) as data_validade
        from {{ ref("fct_estoque_posicao") }}
        group by 1, 2, 3
    ),

    -- TRASNFORMATIONS
    movimento_controlados as (
        select mov.*, mat.controlado_tipo, mat.nome
        from movimento as mov
        inner join controlados as mat using (id_material)
    ),

    eventos as (
        select
            id_cnes,
            id_material,
            nome,
            controlado_tipo,
            id_lote,
            data_evento,
            data_hora_evento,
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
                then
                    concat(
                        "Pedido WMS: ",
                        coalesce(id_pedido_wms, "não registrado (inserção manual)")
                    )
                when movimento_tipo_grupo = "TRANSFERENCIA EXTERNA"
                then "Transferência entre unidades"
                when movimento_tipo_grupo = "CORRECAO DE ESTOQUE / OUTRO"
                then "Correção de lote / Outro"
                when
                    movimento_tipo_grupo = "CONSUMO"
                    and movimento_tipo = "ATENDIMENTO EXTERNO"
                then "Atendimento externo"
                when
                    movimento_tipo_grupo = "CONSUMO"
                    and movimento_tipo in ("REFORÇO", "REFORÇO ISOLADO")
                then "Armário de ermergência"
                when
                    movimento_tipo_grupo = "CONSUMO"
                    and movimento_tipo
                    not in ("REFORÇO", "REFORÇO ISOLADO", "ATENDIMENTO EXTERNO")
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
                when
                    movimento_tipo_grupo = "CONSUMO"
                    and movimento_tipo = "OUTRO"
                then "Correção de lote (diminuição) / Outro"
                when movimento_tipo_grupo = "TRANSFERENCIA EXTERNA"
                then "Transferência entre unidades"
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
                partition by id_cnes, id_material order by data_hora_evento, tipo_evento
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
            mov.movimento_tipo,
            mov.movimento_justificativa,
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

    --
    joined as (
        select
            eventos.*,
            coalesce(posicao_quantidade, 0) as posicao_inicial,
            (
                coalesce(posicao_quantidade, 0) + movimento_quantidade_acumulada
            ) as posicao_final,
            est.nome_limpo as estabelecimento_nome,
            est.area_programatica as estabelecimento_area_programatica,
            concat(
                est.endereco_logradouro, ', ', est.endereco_numero
            ) as estabelecimento_endereco
        from eventos_final as eventos
        left join posicao using (id_cnes, id_material)
        left join estabelecimento as est using (id_cnes)
    ),

    final as (
        select
            id_cnes,
            estabelecimento_nome,
            estabelecimento_area_programatica,
            estabelecimento_endereco,
            id_material,
            upper(
                trim(
                    regexp_replace(
                        regexp_replace(normalize(nome, nfd), r"\pM", ''),
                        r'[^ A-Za-z0-9.,]',
                        ' '
                    )
                )
            ) as nome,
            controlado_tipo,
            id_lote,
            format_date('%d-%m-%Y', data_validade) as data_validade_br,
            if(tipo_evento = "saida", "saída", tipo_evento) as tipo_evento,
            evento,
            movimento_tipo,
            movimento_justificativa,
            data_evento,
            format_date('%d-%m-%Y', data_evento) as data_evento_br,
            ordem,
            -- posicao_inicial,
            movimento_quantidade,
            -- movimento_quantidade_acumulada,
            posicao_final
        from joined
        where data_evento >= (select data from primeiro_dia_mes)
    )

select *
from final
