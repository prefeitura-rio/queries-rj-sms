{{config(schema = 'intermediario_plataforma_subpav',alias = 'cnes_aps__composicao_equipes',materialized = "table",partition_by = {"field": "data_particao","data_type": "date","granularity": "month",},cluster_by = ["ine", "cnes", "tipo_equipe_id"],tags = ["subpav", "cnes_aps"])}}

with equipes as (select
    
    data_particao,
    ano_particao,
    mes_particao,

    ap,
    ap_formatada,
    cnes,
    nome_unidade,
    ine,
    nm_referencia,

    tipo_equipe_id,
    tipo_equipe_descricao,
    classificacao_equipe,
    classificacao_equipe_temporal,
    classificacao_equipe_temporal as classificacao_equipe_historica,
    classificacao_equipe_painel,

    dt_ativacao,
    dt_desativacao,
    equipe_ativa,

    is_esf,
    is_esf_panorama_historico,
    is_eacs,
    is_eacs_panorama_historico,
    is_esb,
    is_ecr,
    is_enasf,
    is_eap,
    is_eapp,
    is_eapp_panorama,
    is_emad,
    is_emad_panorama,
    is_emap,
    is_equipe_aps_painel,
    is_equipe_aps_cobertura,
    is_equipe_aps_historico,
    is_aps

from {{ ref("int_subpav_cnes_aps__equipes") }}
where equipe_ativa = 1
    and ine is not null
    and ine != '0000000000'

),

qpe as (select * from {{ ref("int_subpav_cnes_aps__qpe_com_cbo_ausente") }}),

quantitativos_por_equipe as (
    select 
        data_particao,
        ine,
        tipo_equipe_id,

    -- Médico
    sum(
        case
            when categoria_profissional_composicao_legado = 'MEDICO'
            then total_profissionais else 0
        end
    ) as total_medico,

    sum(
        case
            when categoria_profissional_composicao_legado = 'MEDICO'
            then total_40_horas else 0
        end
    ) as total_medico_40h,

    sum(
        case
            when categoria_profissional_composicao_legado = 'MEDICO'
            then total_20_horas else 0
        end
    ) as total_medico_20h,

    -- Enfermeiro
    sum(
        case
            when categoria_profissional_composicao_legado = 'ENFERMEIRO'
            then total_profissionais else 0
        end
    ) as total_enfermeiro,

    sum(
        case
            when categoria_profissional_composicao_legado = 'ENFERMEIRO'
            then total_40_horas else 0
        end
    ) as total_enfermeiro_40h,

    sum(
        case
            when categoria_profissional_composicao_legado = 'ENFERMEIRO'
            then total_20_horas else 0
        end
    ) as total_enfermeiro_20h,

    -- Técnico/Auxiliar de enfermagem
    sum(
        case
            when categoria_profissional_composicao_legado = 'TEC_AUX_ENFERMAGEM'
            then total_profissionais else 0
        end
    ) as total_tec_aux_enfermagem,

    sum(
        case
            when categoria_profissional_composicao_legado = 'TEC_AUX_ENFERMAGEM'
            then total_40_horas else 0
        end
    ) as total_tec_aux_enfermagem_40h,

    sum(
        case
            when categoria_profissional_composicao_legado = 'TEC_AUX_ENFERMAGEM'
            then total_20_horas else 0
        end
    ) as total_tec_aux_enfermagem_20h,

    -- ACS
    sum(
        case
            when categoria_profissional_composicao_legado = 'ACS'
            then total_profissionais else 0
        end
    ) as total_acs,

    sum(
        case
            when categoria_profissional_composicao_legado = 'ACS'
            then total_40_horas else 0
        end
    ) as total_acs_40h,

    sum(
        case
            when categoria_profissional_composicao_legado = 'ACS'
            then total_20_horas else 0
        end
    ) as total_acs_20h,

    -- Cirurgião dentista
    sum(
        case
            when categoria_profissional_composicao_legado = 'CIRURGIAO_DENTISTA'
            then total_profissionais else 0
        end
    ) as total_cirurgiao_dentista,

    sum(
        case
            when categoria_profissional_composicao_legado = 'CIRURGIAO_DENTISTA'
            then total_40_horas else 0
        end
    ) as total_cirurgiao_dentista_40h,

    sum(
        case
            when categoria_profissional_composicao_legado = 'CIRURGIAO_DENTISTA'
            then total_20_horas else 0
        end
    ) as total_cirurgiao_dentista_20h,

    -- Auxiliar bucal
    sum(
        case
            when categoria_profissional_composicao_legado = 'AUX_BUCAL'
            then total_profissionais else 0
        end
    ) as total_aux_bucal,

    sum(
        case
            when categoria_profissional_composicao_legado = 'AUX_BUCAL'
            then total_40_horas else 0
        end
    ) as total_aux_bucal_40h,

    sum(
        case
            when categoria_profissional_composicao_legado = 'AUX_BUCAL'
            then total_20_horas else 0
        end
    ) as total_aux_bucal_20h,

    -- Técnico bucal
    sum(
        case
            when categoria_profissional_composicao_legado = 'TEC_BUCAL'
            then total_profissionais else 0
        end
    ) as total_tec_bucal,

    sum(
        case
            when categoria_profissional_composicao_legado = 'TEC_BUCAL'
            then total_40_horas else 0
        end
    ) as total_tec_bucal_40h,

    sum(
        case
            when categoria_profissional_composicao_legado = 'TEC_BUCAL'
            then total_20_horas else 0
        end
    ) as total_tec_bucal_20h,

    max(coalesce(inconsistente, 0)) as inconsistente_qpe

from qpe
group by
    data_particao,
    ine,
    tipo_equipe_id

),

flags_incompletude_legado as (
    select
        data_particao,
        ine,
        tipo_equipe_id,

        max(
            case
                when tipo_equipe_id = 70
                    and categoria_profissional_composicao_legado = 'MEDICO'
                    and (
                        total_dias_desligados > 60
                        or date_diff(last_day(data_particao, month), data_eqp_incompleta, day) > 60
                    )
                then 1 else 0
            end
        ) as desligados_60_med,

        max(
            case
                when tipo_equipe_id = 70
                    and categoria_profissional_composicao_legado = 'ENFERMEIRO'
                    and (
                        total_dias_desligados > 60
                        or date_diff(last_day(data_particao, month), data_eqp_incompleta, day) > 60
                    )
                then 1 else 0
            end
        ) as desligados_60_enf,

        max(
            case
                when tipo_equipe_id = 70
                    and categoria_profissional_composicao_legado = 'TEC_AUX_ENFERMAGEM'
                    and (
                        total_dias_desligados > 60
                        or date_diff(last_day(data_particao, month), data_eqp_incompleta, day) > 60
                    )
                then 1 else 0
            end
        ) as desligados_60_aux,

        max(
            case
                when tipo_equipe_id = 70
                    and categoria_profissional_composicao_legado = 'ACS'
                    and (
                        total_dias_desligados > 60
                        or date_diff(last_day(data_particao, month), data_eqp_incompleta, day) > 60
                    )
                then 1 else 0
            end
        ) as desligados_60_acs,

        min(
            case
                when tipo_equipe_id = 70
                    and (
                        total_dias_desligados > 60
                        or date_diff(last_day(data_particao, month), data_eqp_incompleta, day) > 60
                    )
                then coalesce(
                    data_eqp_incompleta,
                    date_sub(
                        last_day(data_particao, month),
                        interval cast(total_dias_desligados as int64) day
                    )
                )
            end
        ) as data_eqp_incompleta,

        min(
            case
                when tipo_equipe_id = 70
                    and coalesce(total_dias_desligados, 0) <= 60
                    and data_eqp_vacancia is not null
                then data_eqp_vacancia
            end
        ) as data_eqp_vacancia

    from qpe
    where tipo_equipe_id = 70
    group by
        data_particao,
        ine,
        tipo_equipe_id
),

flags_vacancia_legado as (
    select 
    data_particao,
    ine,
    tipo_equipe_id,

    max(
        case
            when coalesce(total_profissionais, 0) = 0
                and tipo_equipe_id = 70
                and categoria_profissional_composicao_legado = 'MEDICO'
                and data_eqp_vacancia is not null
            then 1 else 0
        end
    ) as vacancia_medico,

    max(
        case
            when coalesce(total_profissionais, 0) = 0
                and tipo_equipe_id = 70
                and categoria_profissional_composicao_legado = 'ENFERMEIRO'
                and data_eqp_vacancia is not null
            then 1 else 0
        end
    ) as vacancia_enfermeiro,

    max(
        case
            when coalesce(total_profissionais, 0) = 0
                and tipo_equipe_id = 70
                and categoria_profissional_composicao_legado = 'TEC_AUX_ENFERMAGEM'
                and data_eqp_vacancia is not null
            then 1 else 0
        end
    ) as vacancia_tec_aux_enfermagem,

    max(
        case
            when coalesce(total_profissionais, 0) = 0
                and tipo_equipe_id = 70
                and categoria_profissional_composicao_legado = 'ACS'
                and data_eqp_vacancia is not null
            then 1 else 0
        end
    ) as vacancia_acs,

    max(
        case
            when coalesce(total_profissionais, 0) = 0
                and tipo_equipe_id = 71
                and categoria_profissional_composicao_legado = 'CIRURGIAO_DENTISTA'
                and data_eqp_vacancia is not null
            then 1 else 0
        end
    ) as vacancia_cirurgiao_dentista,

    max(
        case
            when coalesce(total_profissionais, 0) = 0
                and tipo_equipe_id = 71
                and categoria_profissional_composicao_legado in ('AUX_BUCAL', 'TEC_BUCAL')
                and data_eqp_vacancia is not null
            then 1 else 0
        end
    ) as vacancia_apoio_bucal

from qpe
where tipo_equipe_id in (70, 71)
group by
    data_particao,
    ine,
    tipo_equipe_id

),

base as (select 
    e.data_particao,
    e.ano_particao,
    e.mes_particao,

    e.ap,
    e.ap_formatada,
    e.cnes,
    e.nome_unidade,
    e.ine,
    e.nm_referencia,

    e.tipo_equipe_id,
    e.classificacao_equipe,

    e.dt_ativacao,
    e.dt_desativacao,
    e.equipe_ativa,

    case
        when e.equipe_ativa = 1
            and e.ap > 0
            and (
                coalesce(e.is_equipe_aps_historico, 0) = 1
                or coalesce(e.is_esb, 0) = 1
                or coalesce(e.is_enasf, 0) = 1
                or coalesce(e.is_eapp_panorama, 0) = 1
                or coalesce(e.is_emad_panorama, 0) = 1
                or coalesce(e.is_emap, 0) = 1
            )
        then 1 else 0
    end as is_equipe_panorama,

    coalesce(e.is_equipe_aps_historico, 0) as is_equipe_aps,
    coalesce(e.is_esf_panorama_historico, 0) as is_equipe_esf,
    coalesce(e.is_esb, 0) as is_equipe_esb,
    coalesce(e.is_enasf, 0) as is_equipe_emulti,
    coalesce(e.is_eapp_panorama, 0) as is_equipe_prisional,
    coalesce(e.is_emad_panorama, 0) as is_equipe_emad,
    coalesce(e.is_emap, 0) as is_equipe_emap,

    case
        when coalesce(e.is_emad_panorama, 0) = 1
            or coalesce(e.is_emap, 0) = 1
        then 1 else 0
    end as is_equipe_ad,

    coalesce(q.total_medico, 0) as total_medico,
    coalesce(q.total_medico_40h, 0) as total_medico_40h,
    coalesce(q.total_medico_20h, 0) as total_medico_20h,

    coalesce(q.total_enfermeiro, 0) as total_enfermeiro,
    coalesce(q.total_enfermeiro_40h, 0) as total_enfermeiro_40h,
    coalesce(q.total_enfermeiro_20h, 0) as total_enfermeiro_20h,

    coalesce(q.total_tec_aux_enfermagem, 0) as total_tec_aux_enfermagem,
    coalesce(q.total_tec_aux_enfermagem_40h, 0) as total_tec_aux_enfermagem_40h,
    coalesce(q.total_tec_aux_enfermagem_20h, 0) as total_tec_aux_enfermagem_20h,

    coalesce(q.total_acs, 0) as total_acs,
    coalesce(q.total_acs_40h, 0) as total_acs_40h,
    coalesce(q.total_acs_20h, 0) as total_acs_20h,

    coalesce(q.total_cirurgiao_dentista, 0) as total_cirurgiao_dentista,
    coalesce(q.total_cirurgiao_dentista_40h, 0) as total_cirurgiao_dentista_40h,
    coalesce(q.total_cirurgiao_dentista_20h, 0) as total_cirurgiao_dentista_20h,

    coalesce(q.total_aux_bucal, 0) as total_aux_bucal,
    coalesce(q.total_aux_bucal_40h, 0) as total_aux_bucal_40h,
    coalesce(q.total_aux_bucal_20h, 0) as total_aux_bucal_20h,

    coalesce(q.total_tec_bucal, 0) as total_tec_bucal,
    coalesce(q.total_tec_bucal_40h, 0) as total_tec_bucal_40h,
    coalesce(q.total_tec_bucal_20h, 0) as total_tec_bucal_20h,

    coalesce(fil.desligados_60_med, 0) as desligados_60_medico,
    coalesce(fil.desligados_60_enf, 0) as desligados_60_enfermeiro,
    coalesce(fil.desligados_60_aux, 0) as desligados_60_tec_aux_enfermagem,
    coalesce(fil.desligados_60_acs, 0) as desligados_60_acs,

    coalesce(vac.vacancia_medico, 0) as vacancia_medico,
    coalesce(vac.vacancia_enfermeiro, 0) as vacancia_enfermeiro,
    coalesce(vac.vacancia_tec_aux_enfermagem, 0) as vacancia_tec_aux_enfermagem,
    coalesce(vac.vacancia_acs, 0) as vacancia_acs,
    coalesce(vac.vacancia_cirurgiao_dentista, 0) as vacancia_cirurgiao_dentista,
    coalesce(vac.vacancia_apoio_bucal, 0) as vacancia_apoio_bucal,

    fil.data_eqp_incompleta,
    fil.data_eqp_vacancia,

    coalesce(q.inconsistente_qpe, 0) as inconsistente_qpe,

    case
        when e.classificacao_equipe_temporal = 'INCONSISTENTE'
            or e.classificacao_equipe_painel = 'INCONSISTENTE'
            then 1
        else 0
    end as inconsistente,

from equipes e
left join quantitativos_por_equipe q
    on e.data_particao = q.data_particao
    and e.ine = q.ine
    and e.tipo_equipe_id = q.tipo_equipe_id
left join flags_incompletude_legado fil
    on e.data_particao = fil.data_particao
    and e.ine = fil.ine
    and e.tipo_equipe_id = fil.tipo_equipe_id
left join flags_vacancia_legado vac
    on e.data_particao = vac.data_particao
    and e.ine = vac.ine
    and e.tipo_equipe_id = vac.tipo_equipe_id

),

regras_esb as (select *,

    -- Regra temporal da composição eSB para compatibilidade com o importador legado.
    -- Até 2026-02, o legado aceita dentista 20h como composição válida.
    -- A partir de 2026-03, passa a exigir dentista 40h.
    case
        when tipo_equipe_id = 71
            and (
                (
                    data_particao < date '2026-03-01'
                    and (
                        total_cirurgiao_dentista_40h > 0
                        or total_cirurgiao_dentista_20h > 0
                    )
                )
                or (
                    data_particao >= date '2026-03-01'
                    and total_cirurgiao_dentista_40h > 0
                )
            )
        then 1
        else 0
    end as esb_tem_cirurgiao_valido,

    -- Até 2026-02, o legado aceita apoio bucal 20h/40h.
    -- A partir de 2026-03, considera apenas ASB/TSB 40h para completude.
    case
        when tipo_equipe_id = 71
            and (
                (
                    data_particao < date '2026-03-01'
                    and (
                        total_aux_bucal_40h > 0
                        or total_aux_bucal_20h > 0
                        or total_tec_bucal_40h > 0
                        or total_tec_bucal_20h > 0
                    )
                )
                or (
                    data_particao >= date '2026-03-01'
                    and (
                        total_aux_bucal_40h > 0
                        or total_tec_bucal_40h > 0
                    )
                )
            )
        then 1
        else 0
    end as esb_tem_apoio_bucal_valido,

    -- Flag de auditoria: identifica eSB diferenciada por carga horária/nomenclatura.
    case
        when tipo_equipe_id = 71
            and (
                total_cirurgiao_dentista_20h > 0
                or total_aux_bucal_20h > 0
                or total_tec_bucal_20h > 0
                or regexp_contains(upper(coalesce(nm_referencia, '')), r'\bDIF\b')
            )
        then 1
        else 0
    end as esb_diferenciada

from base

),

final_base as (
    select
        *,

        -- Incompletudes por categoria, no padrão legado
        desligados_60_medico as incompleta_medico,
        desligados_60_enfermeiro as incompleta_enfermeiro,
        desligados_60_tec_aux_enfermagem as incompleta_tec_aux_enfermagem,
        desligados_60_acs as incompleta_acs,

        case
            when tipo_equipe_id = 70
                and (
                    desligados_60_medico >= 1
                    or desligados_60_enfermeiro >= 1
                    or desligados_60_tec_aux_enfermagem >= 1
                    or desligados_60_acs >= 1
                )
                and (
                    (
                        total_medico_40h = 0
                        and total_medico_20h < 1
                    )
                    or total_enfermeiro_40h = 0
                    or total_tec_aux_enfermagem_40h = 0
                    or total_acs_40h = 0
                )
            then 1
            else 0
        end as esf_incompleta_legado_calculada,

        case
            when tipo_equipe_id = 71
                and esb_tem_cirurgiao_valido = 0
            then 1 else 0
        end as incompleta_cirurgiao_dentista,

        case
            when tipo_equipe_id = 71
                and esb_tem_apoio_bucal_valido = 0
            then 1 else 0
        end as incompleta_apoio_bucal,

        -- Totais de vacância
        (
            vacancia_medico
            + vacancia_enfermeiro
            + vacancia_tec_aux_enfermagem
            + vacancia_acs
        ) as total_vacancia_esf,

        (
            vacancia_cirurgiao_dentista
            + vacancia_apoio_bucal
        ) as total_vacancia_esb,

        -- Totais de incompletude por categoria
        (
            desligados_60_medico
            + desligados_60_enfermeiro
            + desligados_60_tec_aux_enfermagem
            + desligados_60_acs
        ) as total_incompletude_esf,

        (
            case
                when tipo_equipe_id = 71
                    and esb_tem_cirurgiao_valido = 0
                then 1 else 0
            end
            +
            case
                when tipo_equipe_id = 71
                    and esb_tem_apoio_bucal_valido = 0
                then 1 else 0
            end
        ) as total_incompletude_esb

    from regras_esb
),

final as (
    select
        *,

        -- eSF completa/incompleta
        case
            when tipo_equipe_id = 70
                and esf_incompleta_legado_calculada = 0
            then 1
            when tipo_equipe_id = 70
            then 0
            else 0
        end as esf_completa,

        case
            when tipo_equipe_id = 70
                and esf_incompleta_legado_calculada = 1
            then 1
            else 0
        end as esf_incompleta,

        -- ESB completa/incompleta
        case
            when tipo_equipe_id = 71
                and esb_tem_cirurgiao_valido = 1
                and esb_tem_apoio_bucal_valido = 1
            then 1
            when tipo_equipe_id = 71
            then 0
            else 0
        end as esb_completa,

        case
            when tipo_equipe_id = 71
                and not (
                    esb_tem_cirurgiao_valido = 1
                    and esb_tem_apoio_bucal_valido = 1
                )
            then 1
            else 0
        end as esb_incompleta,

        -- APS completa/incompleta
        case
            when tipo_equipe_id = 70
                and esf_incompleta_legado_calculada = 0
            then 1
            when tipo_equipe_id in (73, 76)
            then 1
            else 0
        end as aps_completa,

        case
            when tipo_equipe_id = 70
                and esf_incompleta_legado_calculada = 1
            then 1
            else 0
        end as aps_incompleta

    from final_base
)

select * from final
