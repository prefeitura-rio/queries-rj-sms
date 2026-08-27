{{
    config(
        schema = 'intermediario_plataforma_subpav',
        alias = 'cnes_aps__composicao_equipes',
        materialized = "table",
        partition_by = {
            "field": "data_particao",
            "data_type": "date",
            "granularity": "month",
        },
        cluster_by = ["ine", "cnes", "tipo_equipe_id"],
        tags = ["subpav", "cnes_aps"]
    )
}}

with competencias as (
    select
        data_particao,
        competencia,
        competencia_id,
        dt_final_competencia,
        dt_final_competencia_anterior
    from {{ ref("int_subpav_cnes_aps__competencias_legado") }}
),

equipes as (
    select
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
        e.tipo_equipe_descricao,
        e.classificacao_equipe,
        e.dt_ativacao,
        e.dt_desativacao,
        e.equipe_ativa,
        e.is_esf,
        e.is_esf_panorama_historico,
        e.is_eacs_panorama_historico,
        e.is_esb,
        e.is_ecr,
        e.is_eap,
        e.is_equipe_aps_historico,
        e.is_equipe_aps_painel,
        e.is_aps,

        c.competencia,
        c.competencia_id,
        c.dt_final_competencia,
        c.dt_final_competencia_anterior

    from {{ ref("int_subpav_cnes_aps__equipes") }} e

    inner join competencias c
        on c.data_particao = e.data_particao

    where e.equipe_ativa = 1
        and e.ine is not null
        and e.ine != '0000000000'
),

qpe as (
    select
        q.*,
        c.competencia,
        c.competencia_id,
        c.dt_final_competencia,

        case
            when q.total_dias_desligados > 60
                or date_diff(c.dt_final_competencia, q.data_eqp_incompleta, day) > 60
                then 1
            else 0
        end as tem_impacto_60d

    from {{ ref("int_subpav_cnes_aps__qpe_com_cbo_ausente") }} q

    inner join competencias c
        on c.data_particao = q.data_particao
),

agrupa_por_ine as (
    select
        data_particao,
        ine,
        tipo_equipe_id,

        sum(case when categoria_profissional_composicao = 'MEDICO' then total_profissionais else 0 end) as total_medico,
        sum(case when categoria_profissional_composicao = 'MEDICO' then total_40_horas else 0 end) as medico,
        sum(case when categoria_profissional_composicao = 'MEDICO' then total_20_horas else 0 end) as medico_20,

        sum(case when categoria_profissional_composicao = 'ENFERMEIRO' then total_profissionais else 0 end) as total_enfermeiro,
        sum(case when categoria_profissional_composicao = 'ENFERMEIRO' then total_40_horas else 0 end) as enfermeiro,

        sum(case when categoria_profissional_composicao = 'TEC_AUX_ENFERMAGEM' then total_profissionais else 0 end) as total_tec_aux,
        sum(case when categoria_profissional_composicao = 'TEC_AUX_ENFERMAGEM' then total_40_horas else 0 end) as tec_aux,

        sum(case when categoria_profissional_composicao = 'ACS' then total_profissionais else 0 end) as total_acs,
        sum(case when categoria_profissional_composicao = 'ACS' then total_40_horas else 0 end) as acs,

        sum(case when categoria_profissional_composicao = 'CIRURGIAO_DENTISTA' then total_40_horas else 0 end) as cirurgiao,
        sum(case when categoria_profissional_composicao = 'CIRURGIAO_DENTISTA' then total_20_horas else 0 end) as cirurgiao_20,

        sum(case when categoria_profissional_composicao = 'AUX_BUCAL' then total_40_horas else 0 end) as aux_bucal,
        sum(case when categoria_profissional_composicao = 'AUX_BUCAL' then total_20_horas else 0 end) as aux_bucal_20,

        sum(case when categoria_profissional_composicao = 'TEC_BUCAL' then total_40_horas else 0 end) as tec_bucal,
        sum(case when categoria_profissional_composicao = 'TEC_BUCAL' then total_20_horas else 0 end) as tec_bucal_20,

        sum(
            case
                when categoria_profissional_composicao = 'MEDICO'
                    and tem_impacto_60d = 1
                    then 1
                else 0
            end
        ) as desligados_60_med,

        sum(
            case
                when categoria_profissional_composicao = 'ENFERMEIRO'
                    and tem_impacto_60d = 1
                    then 1
                else 0
            end
        ) as desligados_60_enf,

        sum(
            case
                when categoria_profissional_composicao = 'TEC_AUX_ENFERMAGEM'
                    and tem_impacto_60d = 1
                    then 1
                else 0
            end
        ) as desligados_60_aux,

        sum(
            case
                when categoria_profissional_composicao = 'ACS'
                    and tem_impacto_60d = 1
                    then 1
                else 0
            end
        ) as desligados_60_acs,

        sum(
            case
                when categoria_profissional_composicao = 'CIRURGIAO_DENTISTA'
                    and tem_impacto_60d = 1
                    then 1
                else 0
            end
        ) as desligados_60_dent,

        sum(
            case
                when categoria_profissional_composicao = 'AUX_BUCAL'
                    and tem_impacto_60d = 1
                    then 1
                else 0
            end
        ) as desligados_60_aux_bucal,

        sum(
            case
                when categoria_profissional_composicao = 'TEC_BUCAL'
                    and tem_impacto_60d = 1
                    then 1
                else 0
            end
        ) as desligados_60_tec_bucal,

        min(
            case
                when categoria_profissional_composicao in (
                    'MEDICO',
                    'ENFERMEIRO',
                    'TEC_AUX_ENFERMAGEM',
                    'ACS',
                    'CIRURGIAO_DENTISTA',
                    'AUX_BUCAL',
                    'TEC_BUCAL'
                )
                    and tem_impacto_60d = 1
                    then data_eqp_incompleta
            end
        ) as data_eqp_incompleta,

        min(
            case
                when tem_impacto_60d = 1
                    then data_eqp_vacancia
            end
        ) as data_eqp_vacancia,

        max(coalesce(inconsistente, 0)) as inconsistente_qpe

    from qpe

    group by
        data_particao,
        ine,
        tipo_equipe_id
),

base as (
    select
        e.*,

        coalesce(a.total_medico, 0) as total_medico,
        coalesce(a.medico, 0) as medico,
        coalesce(a.medico_20, 0) as medico_20,

        coalesce(a.total_enfermeiro, 0) as total_enfermeiro,
        coalesce(a.enfermeiro, 0) as enfermeiro,

        coalesce(a.total_tec_aux, 0) as total_tec_aux,
        coalesce(a.tec_aux, 0) as tec_aux,

        coalesce(a.total_acs, 0) as total_acs,
        coalesce(a.acs, 0) as acs,

        coalesce(a.cirurgiao, 0) as cirurgiao,
        coalesce(a.cirurgiao_20, 0) as cirurgiao_20,
        coalesce(a.aux_bucal, 0) as aux_bucal,
        coalesce(a.aux_bucal_20, 0) as aux_bucal_20,
        coalesce(a.tec_bucal, 0) as tec_bucal,
        coalesce(a.tec_bucal_20, 0) as tec_bucal_20,

        coalesce(a.desligados_60_med, 0) as desligados_60_med,
        coalesce(a.desligados_60_enf, 0) as desligados_60_enf,
        coalesce(a.desligados_60_aux, 0) as desligados_60_aux,
        coalesce(a.desligados_60_acs, 0) as desligados_60_acs,

        coalesce(a.desligados_60_dent, 0) as desligados_60_dent,
        coalesce(a.desligados_60_aux_bucal, 0) as desligados_60_aux_bucal,
        coalesce(a.desligados_60_tec_bucal, 0) as desligados_60_tec_bucal,
        a.data_eqp_incompleta,
        a.data_eqp_vacancia,

        0 as inconsistente,
        inconsistente_qpe,

        coalesce(e.is_esf_panorama_historico, 0) as is_tipo_esf_panorama,

        case
            when (
                e.competencia_id <= 163
                and coalesce(a.acs, 0) >= 4
            )
                or (
                    e.competencia_id > 163
                    and coalesce(a.acs, 0) >= 1
                )
                then 1
            else 0
        end as acs_minimo_ok

    from equipes e

    inner join agrupa_por_ine a
        on a.data_particao = e.data_particao
        and a.ine = e.ine
        and a.tipo_equipe_id = e.tipo_equipe_id
),

define_tipagem_esf as (
    select
        *,

        case
            when is_tipo_esf_panorama = 1
                and (
                    (
                        medico = 0
                        and medico_20 < 2
                        and desligados_60_med >= 1
                    )
                    or (
                        enfermeiro < 1
                        and desligados_60_enf >= 1
                    )
                    or (
                        tec_aux < 1
                        and desligados_60_aux >= 1
                    )
                    or (
                        acs_minimo_ok = 0
                        and desligados_60_acs >= 1
                    )
                )
                then 1
            else 0
        end as esf_incompleta_php

    from base
),

define_tipagem_esb as (
    select
        *,

        case
            when is_esb = 1
                and cirurgiao >= 1
                and (
                    aux_bucal >= 1
                    or tec_bucal >= 1
                )
                then 1
            else 0
        end as esb_completa_40h_calculada,

        case
            when is_esb = 1
                and (
                    cirurgiao >= 1
                    or cirurgiao_20 >= 1
                )
                and (
                    aux_bucal >= 1
                    or aux_bucal_20 >= 1
                    or tec_bucal >= 1
                    or tec_bucal_20 >= 1
                )
                and not (
                    cirurgiao >= 1
                    and (
                        aux_bucal >= 1
                        or tec_bucal >= 1
                    )
                )
                then 1
            else 0
        end as esb_diferenciada_calculada,

        case
            when is_esb = 1
                and (
                    cirurgiao >= 1
                    or cirurgiao_20 >= 1
                )
                and (
                    aux_bucal >= 1
                    or aux_bucal_20 >= 1
                    or tec_bucal >= 1
                    or tec_bucal_20 >= 1
                )
                then 1
            else 0
        end as esb_completa_calculada,

        case
            when is_esb = 1
                and not (
                    (
                        cirurgiao >= 1
                        or cirurgiao_20 >= 1
                    )
                    and (
                        aux_bucal >= 1
                        or aux_bucal_20 >= 1
                        or tec_bucal >= 1
                        or tec_bucal_20 >= 1
                    )
                )
                then 1
            else 0
        end as esb_incompleta_calculada,

        case
            when is_esb = 1
                and cirurgiao < 1
                and cirurgiao_20 < 1
                then 1
            else 0
        end as incompleta_cirurgiao_dentista,

        case
            when is_esb = 1
                and aux_bucal < 1
                and aux_bucal_20 < 1
                and tec_bucal < 1
                and tec_bucal_20 < 1
                then 1
            else 0
        end as incompleta_aux_tec_bucal,

        case
            when is_esb = 1
                and (
                    (
                        cirurgiao < 1
                        and cirurgiao_20 < 1
                        and desligados_60_dent >= 1
                    )
                    or (
                        aux_bucal < 1
                        and aux_bucal_20 < 1
                        and tec_bucal < 1
                        and tec_bucal_20 < 1
                        and (
                            desligados_60_aux_bucal >= 1
                            or desligados_60_tec_bucal >= 1
                        )
                    )
                )
                then 1
            else 0
        end as esb_inexistente_60d

    from define_tipagem_esf
)

select
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
    dt_ativacao,
    dt_desativacao,
    equipe_ativa,
    is_esf,
    is_esb,
    is_aps,
    is_esf_panorama_historico,
    is_eacs_panorama_historico,
    is_ecr,
    is_eap,
    is_equipe_aps_historico,
    is_equipe_aps_painel,

    competencia,
    competencia_id,
    dt_final_competencia,
    dt_final_competencia_anterior,

    total_medico,
    medico as total_medico_40h,
    medico_20 as total_medico_20h,

    total_enfermeiro,
    enfermeiro as total_enfermeiro_40h,

    total_tec_aux,
    tec_aux as total_tec_aux_enfermagem_40h,

    total_acs,
    acs as total_acs_40h,

    cirurgiao,
    cirurgiao_20,
    aux_bucal,
    aux_bucal_20,
    tec_bucal,
    tec_bucal_20,

    desligados_60_med as desligados_60_medico,
    desligados_60_enf as desligados_60_enfermeiro,
    desligados_60_aux as desligados_60_tec_aux_enfermagem,
    desligados_60_acs,

    data_eqp_incompleta,
    data_eqp_vacancia,

    case
        when is_tipo_esf_panorama = 1
            and medico = 0
            and medico_20 < 2
            and desligados_60_med > 0
            then 1
        else 0
    end as incompleta_medico,

    case
        when desligados_60_enf >= 1
            and enfermeiro < 1
            then 1
        else 0
    end as incompleta_enfermeiro,

    case
        when desligados_60_aux >= 1
            and tec_aux < 1
            then 1
        else 0
    end as incompleta_tec_aux_enfermagem,

    case
        when desligados_60_acs >= 1
            and acs_minimo_ok = 0
            then 1
        else 0
    end as incompleta_acs,

    (
        case
            when desligados_60_med >= 1
                and medico = 0
                and medico_20 < 2
                then 1
            else 0
        end
        + case
            when desligados_60_enf >= 1
                and enfermeiro < 1
                then 1
            else 0
        end
        + case
            when desligados_60_aux >= 1
                and tec_aux < 1
                then 1
            else 0
        end
        + case
            when desligados_60_acs >= 1
                and acs_minimo_ok = 0
                then 1
            else 0
        end
    ) as total_incompletude_esf,

    case
        when is_tipo_esf_panorama = 1
            and esf_incompleta_php = 0
            then 1
        else 0
    end as esf_completa,

    case
        when is_tipo_esf_panorama = 1
            and esf_incompleta_php = 1
            then 1
        else 0
    end as esf_incompleta,

    case
        when is_equipe_aps_historico = 1
            and esf_incompleta_php = 0
            then 1
        else 0
    end as aps_completa,

    case
        when is_equipe_aps_historico = 1
            and esf_incompleta_php = 1
            then 1
        else 0
    end as aps_incompleta,

    esb_completa_40h_calculada as esb_completa_40h,
    esb_diferenciada_calculada as esb_diferenciada,
    esb_completa_calculada as esb_completa,
    esb_incompleta_calculada as esb_incompleta,

    desligados_60_dent as desligados_60_cirurgiao_dentista,
    desligados_60_aux_bucal,
    desligados_60_tec_bucal,

    incompleta_cirurgiao_dentista,
    incompleta_aux_tec_bucal,
    esb_inexistente_60d,

    0 as inconsistente,
    inconsistente_qpe,

    is_tipo_esf_panorama,
    acs_minimo_ok

from define_tipagem_esb
