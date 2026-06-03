{{
    config(
        schema = 'intermediario_plataforma_subpav',
        alias = 'cnes_aps__quantitativos_profissionais_equipes',
        materialized = "table",
        partition_by = {
            "field": "data_particao",
            "data_type": "date",
            "granularity": "month",
        },
        cluster_by = ["ine", "cnes", "cod_cbo"],
        tags = ["subpav", "cnes_aps"]
    )
}}

with profissionais_consolidacao as (
    select *
    from {{ ref("int_subpav_cnes_aps__profissionais_consolidacao") }}
    where possui_vinculo_equipe = 1
        and equipe_ativa = 1
        and ine is not null
        and cod_cbo is not null
        and cg_horaamb >= 20
),

mapa_cbo_composicao_legado as (
    select '2231F9' as cod_cbo, 'MEDICO' as categoria_profissional_composicao_legado union all
    select '225103', 'MEDICO' union all
    select '225105', 'MEDICO' union all
    select '225106', 'MEDICO' union all
    select '225109', 'MEDICO' union all
    select '225110', 'MEDICO' union all
    select '225112', 'MEDICO' union all
    select '225115', 'MEDICO' union all
    select '225118', 'MEDICO' union all
    select '225120', 'MEDICO' union all
    select '225121', 'MEDICO' union all
    select '225122', 'MEDICO' union all
    select '225124', 'MEDICO' union all
    select '225125', 'MEDICO' union all
    select '225127', 'MEDICO' union all
    select '225130', 'MEDICO' union all
    select '225133', 'MEDICO' union all
    select '225135', 'MEDICO' union all
    select '225136', 'MEDICO' union all
    select '225139', 'MEDICO' union all
    select '225140', 'MEDICO' union all
    select '225142', 'MEDICO' union all
    select '225145', 'MEDICO' union all
    select '225148', 'MEDICO' union all
    select '225150', 'MEDICO' union all
    select '225151', 'MEDICO' union all
    select '225154', 'MEDICO' union all
    select '225155', 'MEDICO' union all
    select '225160', 'MEDICO' union all
    select '225165', 'MEDICO' union all
    select '225170', 'MEDICO' union all
    select '225175', 'MEDICO' union all
    select '225180', 'MEDICO' union all
    select '225185', 'MEDICO' union all
    select '225195', 'MEDICO' union all

    select '223505', 'ENFERMEIRO' union all
    select '223510', 'ENFERMEIRO' union all
    select '223515', 'ENFERMEIRO' union all
    select '223520', 'ENFERMEIRO' union all
    select '223525', 'ENFERMEIRO' union all
    select '223530', 'ENFERMEIRO' union all
    select '223535', 'ENFERMEIRO' union all
    select '223540', 'ENFERMEIRO' union all
    select '223545', 'ENFERMEIRO' union all
    select '223550', 'ENFERMEIRO' union all
    select '223555', 'ENFERMEIRO' union all
    select '223560', 'ENFERMEIRO' union all
    select '223565', 'ENFERMEIRO' union all
    select '223570', 'ENFERMEIRO' union all
    select '2235C3', 'ENFERMEIRO' union all

    select '322205', 'TEC_AUX_ENFERMAGEM' union all
    select '322230', 'TEC_AUX_ENFERMAGEM' union all
    select '322245', 'TEC_AUX_ENFERMAGEM' union all
    select '322250', 'TEC_AUX_ENFERMAGEM' union all

    select '515105', 'ACS' union all
    select '322255', 'ACS' union all

    select '223208', 'CIRURGIAO_DENTISTA' union all
    select '223293', 'CIRURGIAO_DENTISTA' union all

    select '322415', 'AUX_BUCAL' union all
    select '322430', 'AUX_BUCAL' union all

    select '322405', 'TEC_BUCAL' union all
    select '322425', 'TEC_BUCAL'
),

mapa_cbo_vacancia_panorama as (
    select '225125' as cod_cbo, 'MEDICO' as categoria_profissional_vacancia_panorama union all
    select '225142', 'MEDICO' union all

    select '223505', 'ENFERMEIRO' union all
    select '223565', 'ENFERMEIRO' union all

    select '322205', 'TEC_AUX_ENFERMAGEM' union all
    select '322245', 'TEC_AUX_ENFERMAGEM' union all

    select '515105', 'ACS' union all

    select '223293', 'CIRURGIAO_DENTISTA' union all

    select '322430', 'AUX_BUCAL' union all

    select '322405', 'TEC_BUCAL' union all
    select '322425', 'TEC_BUCAL'
),

classificado as (
    select
        pc.*,

        comp.categoria_profissional_composicao_legado,
        vac.categoria_profissional_vacancia_panorama,

        -- aliases de compatibilidade com o que já tínhamos usado
        vac.categoria_profissional_vacancia_panorama as categoria_profissional_panorama,

        case
            when regexp_contains(pc.cod_cbo, r'^2251') then 'MEDICO'
            when pc.cod_cbo in ('223565', '223505') then 'ENFERMEIRO'
            when pc.cod_cbo in ('322245', '322230', '322250', '322205', '322255') then 'TEC_AUX_ENFERMAGEM'
            when pc.cod_cbo in ('515105', '515140', '322255') then 'ACS'
            when pc.cod_cbo in ('223208', '223272', '223288', '223293') then 'CIRURGIAO_DENTISTA'
            when pc.cod_cbo in ('322415', '322430') then 'AUX_BUCAL'
            when pc.cod_cbo in ('322405', '322425') then 'TEC_BUCAL'
            else 'OUTROS'
        end as categoria_profissional_aps_ampla,

        coalesce(
            comp.categoria_profissional_composicao_legado,
            case
                when regexp_contains(pc.cod_cbo, r'^2251') then 'MEDICO'
                when pc.cod_cbo in ('223565', '223505') then 'ENFERMEIRO'
                when pc.cod_cbo in ('322245', '322230', '322250', '322205', '322255') then 'TEC_AUX_ENFERMAGEM'
                when pc.cod_cbo in ('515105', '515140', '322255') then 'ACS'
                when pc.cod_cbo in ('223208', '223272', '223288', '223293') then 'CIRURGIAO_DENTISTA'
                when pc.cod_cbo in ('322415', '322430') then 'AUX_BUCAL'
                when pc.cod_cbo in ('322405', '322425') then 'TEC_BUCAL'
                else 'OUTROS'
            end
        ) as categoria_profissional_aps,

        case
            when comp.cod_cbo is not null then 1
            else 0
        end as is_cbo_composicao_legado,

        case
            when vac.cod_cbo is not null then 1
            else 0
        end as is_cbo_vacancia_panorama,

        -- alias de compatibilidade
        case
            when vac.cod_cbo is not null then 1
            else 0
        end as is_cbo_panorama

    from profissionais_consolidacao pc
    left join mapa_cbo_composicao_legado comp
        on pc.cod_cbo = comp.cod_cbo
    left join mapa_cbo_vacancia_panorama vac
        on pc.cod_cbo = vac.cod_cbo
),

agregado as (
    select
        data_particao,
        ano_particao,
        mes_particao,
        competencia_mes,

        ap,
        ap_formatada,
        cnes,
        nome_unidade,
        ine,

        tipo_equipe_id,
        classificacao_equipe,
        classificacao_equipe_historica,
        classificacao_equipe_painel,

        is_equipe_aps_cobertura,
        is_equipe_aps_cobertura_historica,
        is_equipe_aps_cobertura_painel,
        is_equipe_saude_bucal,
        is_equipe_emulti,
        is_equipe_prisional,
        is_equipe_ad,
        is_aps,

        cod_cbo,

        categoria_profissional_composicao_legado,
        categoria_profissional_vacancia_panorama,
        categoria_profissional_panorama,
        categoria_profissional_aps_ampla,
        categoria_profissional_aps,

        is_cbo_composicao_legado,
        is_cbo_vacancia_panorama,
        is_cbo_panorama,

        count(*) as total_registros,
        countif(dt_desligamento is null) as total_profissionais,

        countif(sexo_id = 1 and dt_desligamento is null) as total_prof_masculino,
        countif(sexo_id = 2 and dt_desligamento is null) as total_prof_feminino,
        countif((sexo_id is null or sexo_id = 0) and dt_desligamento is null) as total_sem_sexo,

        countif(cg_horaamb >= 40 and dt_desligamento is null) as total_40_horas,

        countif(
            cg_horaamb >= 20
            and cg_horaamb < 40
            and dt_desligamento is null
        ) as total_20_horas,

        sum(
            case
                when dt_desligamento is null then coalesce(cg_horaamb, 0)
                else 0
            end
        ) as total_carga_horaria_ambulatorial,

        countif(cg_horaamb = 24 and dt_desligamento is null) as total_24_horas,

        countif(
            cg_horaamb not in (20, 24, 40)
            and dt_desligamento is null
        ) as total_outras_horas,

        countif(tp_residente = 1 and dt_desligamento is null) as total_residentes,
        countif(tp_preceptor = 1 and dt_desligamento is null) as total_preceptores,

        countif(
            format_date('%Y%m', dt_entrada) = competencia_mes
            and dt_desligamento is null
        ) as total_contratados,

        countif(dt_desligamento is not null) as total_desligados,

        case
            when countif(cg_horaamb >= 40 and dt_desligamento is null) = 0
                then max(dias_desligado)
            else null
        end as total_dias_desligados,

        case
            when countif(cg_horaamb >= 40 and dt_desligamento is null) = 0
                and coalesce(max(dias_desligado), 0) > 60
                then max(dt_desligamento)
            else null
        end as data_eqp_incompleta,

        case
            when countif(cg_horaamb >= 40 and dt_desligamento is null) = 0
                and coalesce(max(dias_desligado), 0) <= 60
                then max(dt_desligamento)
            else null
        end as data_eqp_vacancia,

        max(inconsistente) as inconsistente

    from classificado
    group by
        data_particao,
        ano_particao,
        mes_particao,
        competencia_mes,
        ap,
        ap_formatada,
        cnes,
        nome_unidade,
        ine,
        tipo_equipe_id,
        classificacao_equipe,
        classificacao_equipe_historica,
        classificacao_equipe_painel,
        is_equipe_aps_cobertura,
        is_equipe_aps_cobertura_historica,
        is_equipe_aps_cobertura_painel,
        is_equipe_saude_bucal,
        is_equipe_emulti,
        is_equipe_prisional,
        is_equipe_ad,
        is_aps,
        cod_cbo,
        categoria_profissional_composicao_legado,
        categoria_profissional_vacancia_panorama,
        categoria_profissional_panorama,
        categoria_profissional_aps_ampla,
        categoria_profissional_aps,
        is_cbo_composicao_legado,
        is_cbo_vacancia_panorama,
        is_cbo_panorama
)

select *
from agregado