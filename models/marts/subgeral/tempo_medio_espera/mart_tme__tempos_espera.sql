-- noqa: disable=LT08
{{
    config(
        enabled=true,
        schema="projeto_sisreg_tme",
        alias="tempos_espera_individuais",
        partition_by={
            "field": "data_execucao",
            "data_type": "date",
            "granularity": "month",
        },
    )
}}

with

    dim_estabs as (
        select
            id_cnes,
            nome_fantasia,
            id_ap,
            ap,
            endereco_bairro,
            esfera,
            tipo_unidade_agrupado as unidade_tp_agg,
            tipo_unidade_alternativo as unidade_tp

        from
            (
                select
                    *,
                    row_number() over (
                        partition by id_cnes
                        order by ano_competencia desc, mes_competencia desc
                    ) as row_num
                from {{ ref("dim_estabelecimento_sus_rio_historico") }}
            )
        where row_num = 1
    ),

    proceds as (
        select id_procedimento, descricao
        from {{ ref("raw_sheets__assistencial_procedimento") }}
    ),

    tempos_espera as (
        select
            id_solicitacao,
            data_solicitacao,
            data_execucao,
            id_cnes_unidade_solicitante as solicitante_id_cnes,
            id_cnes_unidade_executante as executante_id_cnes,
            id_procedimento_sisreg as procedimento_id,
            date_diff(data_execucao, data_solicitacao, day) as tempo_espera

        from {{ ref("mart_sisreg__solicitacoes") }}
        where
            1 = 1
            and data_solicitacao is not null
            and data_execucao is not null
            and data_execucao >= data_solicitacao
            and vaga_consumida_tp in ("1 VEZ", "RESERVA TECNICA")
            and id_procedimento_sisreg not like "%PPI%"
            and solicitacao_status not in (
                "AGENDAMENTO / CANCELADO / SOLICITANTE",
                "AGENDAMENTO / CANCELADO / REGULADOR",
                "AGENDAMENTO / CANCELADO / COORDENADOR"
            )

    ),

    final as (
        select
            t.id_solicitacao,
            t.data_solicitacao,
            t.data_execucao,
            t.solicitante_id_cnes,
            s.nome_fantasia as nome_fantasia_sol,
            s.id_ap as id_ap_sol,
            s.ap as ap_sol,
            s.endereco_bairro as endereco_bairro_sol,
            s.esfera as esfera_sol,
            s.unidade_tp_agg as unidade_tp_agg_sol,
            s.unidade_tp as unidade_tp_sol,
            t.executante_id_cnes,
            e.nome_fantasia as nome_fantasia_exec,
            e.id_ap as id_ap_exec,
            e.ap as ap_exec,
            e.endereco_bairro as endereco_bairro_exec,
            e.esfera as esfera_exec,
            e.unidade_tp_agg as unidade_tp_agg_exec,
            e.unidade_tp as unidade_tp_exec,
            t.procedimento_id,
            p.descricao as procedimento_descricao,
            t.tempo_espera
        from tempos_espera t
        left join dim_estabs s on t.solicitante_id_cnes = s.id_cnes
        left join dim_estabs e on t.executante_id_cnes = e.id_cnes
        left join proceds p on t.procedimento_id = p.id_procedimento
    )

select *
from final
