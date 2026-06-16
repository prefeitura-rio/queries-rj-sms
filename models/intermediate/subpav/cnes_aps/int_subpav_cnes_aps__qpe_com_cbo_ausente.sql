{{
    config(
        schema = 'intermediario_plataforma_subpav',
        alias = 'cnes_aps__qpe_com_cbo_ausente',
        materialized = "table",
        partition_by = {
            "field": "data_particao",
            "data_type": "date",
            "granularity": "month",
        },
        cluster_by = ["ine", "tipo_equipe_id", "cod_cbo"],
        tags = ["subpav", "cnes_aps"]
    )
}}

with qpe_base as (
    select
        q.*,
        'ORIGINAL' as origem_linha_qpe
    from {{ ref("int_subpav_cnes_aps__quantitativos_profissionais_equipes") }} q
),

competencias as (
    select
        competencia_id,
        data_particao,
        competencia,
        dt_final_competencia,
        dt_final_competencia_anterior
    from {{ ref("int_subpav_cnes_aps__competencias_legado") }}
    where dt_final_competencia is not null
),

equipes as (
    select
        e.data_particao,
        e.ap,
        e.ap_formatada,
        e.cnes,
        e.nome_unidade,
        e.ine,
        e.nm_referencia,
        e.tipo_equipe_id,
        e.classificacao_equipe,
        e.dt_ativacao,
        cl.competencia_id,
        cl.dt_final_competencia,
        cl.dt_final_competencia_anterior
    from {{ ref("int_subpav_cnes_aps__equipes") }} e
    left join competencias cl
        on e.data_particao = cl.data_particao
    where e.equipe_ativa = 1
        and e.tipo_equipe_id in (70, 71)
),

mapa_composicao as (
    select 70 as tipo_equipe_id, 'MEDICO' as categoria, '225142' as cod_cbo_sintetico, 'ESF' as grupo_tipo_equipe union all
    select 70, 'ENFERMEIRO', '223565', 'ESF' union all
    select 70, 'TEC_AUX_ENFERMAGEM', '322250', 'ESF' union all
    select 70, 'ACS', '515105', 'ESF' union all
    select 71, 'CIRURGIAO_DENTISTA', '223293', 'ESB' union all
    select 71, 'AUX_BUCAL', '322430', 'ESB' union all
    select 71, 'TEC_BUCAL', '322425', 'ESB'
),

qpe_categorizado as (
    select
        q.*,
        case
            when q.cod_cbo like '2251%' or q.cod_cbo = '2231F9' then 'MEDICO'
            when q.cod_cbo like '2235%' then 'ENFERMEIRO'
            when q.cod_cbo in ('322205', '322245', '322230', '322250') then 'TEC_AUX_ENFERMAGEM'
            when q.cod_cbo in ('515105', '322255') then 'ACS'
            when q.cod_cbo in ('223208', '223293') then 'CIRURGIAO_DENTISTA'
            when q.cod_cbo in ('322415', '322430') then 'AUX_BUCAL'
            when q.cod_cbo in ('322405', '322425') then 'TEC_BUCAL'
        end as categoria_composicao_sintetica
    from qpe_base q
),

presenca_categoria as (
    select
        q.data_particao,
        q.ine,
        q.tipo_equipe_id,
        q.categoria_composicao_sintetica as categoria,
        count(*) as qtd_linhas_categoria,
        sum(coalesce(q.total_profissionais, 0)) as total_profissionais_categoria,
        sum(coalesce(q.total_40_horas, 0)) as total_40h_categoria,
        sum(coalesce(q.total_20_horas, 0)) as total_20h_categoria,
        min(
        case
            when coalesce(q.total_profissionais, 0) = 0
            then q.data_eqp_vacancia
        end
        ) as data_eqp_vacancia_categoria,

        min(
        case
            when coalesce(q.total_profissionais, 0) = 0
            then q.data_eqp_incompleta
        end
        ) as data_eqp_incompleta_categoria
    from qpe_categorizado q
    where q.categoria_composicao_sintetica is not null
    group by
        q.data_particao,
        q.ine,
        q.tipo_equipe_id,
        categoria
),

categorias_esperadas as (
    select
        e.data_particao,
        e.ap,
        e.ap_formatada,
        e.cnes,
        e.nome_unidade,
        e.ine,
        e.nm_referencia,
        e.tipo_equipe_id,
        e.classificacao_equipe,
        e.dt_ativacao,
        e.competencia_id,
        e.dt_final_competencia,
        e.dt_final_competencia_anterior,
        m.categoria,
        m.cod_cbo_sintetico,
        m.grupo_tipo_equipe
    from equipes e
    inner join mapa_composicao m
        on e.tipo_equipe_id = m.tipo_equipe_id
),

status_categoria_mes_base as (
    select
        ce.data_particao,
        ce.ap,
        ce.ap_formatada,
        ce.cnes,
        ce.nome_unidade,
        ce.ine,
        ce.nm_referencia,
        ce.tipo_equipe_id,
        ce.classificacao_equipe,
        ce.dt_ativacao,
        ce.competencia_id,
        ce.dt_final_competencia,
        ce.dt_final_competencia_anterior,
        ce.categoria,
        ce.cod_cbo_sintetico,
        ce.grupo_tipo_equipe,
        coalesce(pc.qtd_linhas_categoria, 0) as qtd_linhas_categoria,
        coalesce(pc.total_profissionais_categoria, 0) as total_profissionais_categoria,
        coalesce(pc.total_40h_categoria, 0) as total_40h_categoria,
        coalesce(pc.total_20h_categoria, 0) as total_20h_categoria,
        pc.data_eqp_vacancia_categoria,
        pc.data_eqp_incompleta_categoria
    from categorias_esperadas ce
    left join presenca_categoria pc
        on ce.data_particao = pc.data_particao
        and ce.ine = pc.ine
        and ce.tipo_equipe_id = pc.tipo_equipe_id
        and ce.categoria = pc.categoria
),

status_categoria_mes as (
    select
        *,
        case
            when tipo_equipe_id = 70
                and categoria = 'MEDICO'
                and (
                    total_40h_categoria > 0
                    or total_20h_categoria >= 2
                )
                then 1
            when tipo_equipe_id = 70
                and categoria in ('ENFERMEIRO', 'TEC_AUX_ENFERMAGEM', 'ACS')
                and total_40h_categoria > 0
                then 1
            when tipo_equipe_id != 70
                and total_profissionais_categoria > 0
                then 1
            else 0
        end as categoria_valida_composicao
    from status_categoria_mes_base
),

categorias_ausentes as (
    select
        data_particao,
        ap,
        ap_formatada,
        cnes,
        nome_unidade,
        ine,
        nm_referencia,
        tipo_equipe_id,
        classificacao_equipe,
        dt_ativacao,
        competencia_id,
        dt_final_competencia,
        dt_final_competencia_anterior,
        categoria,
        cod_cbo_sintetico,
        grupo_tipo_equipe,
        qtd_linhas_categoria,
        total_profissionais_categoria,
        total_40h_categoria,
        total_20h_categoria,
        categoria_valida_composicao,
        data_eqp_vacancia_categoria,
        data_eqp_incompleta_categoria,

        case
            when qtd_linhas_categoria = 0 then 1
            else 0
        end as is_categoria_totalmente_ausente

    from status_categoria_mes
    where categoria_valida_composicao = 0
),

profissionais_cbo_sintetico as (
    select
        data_particao,
        origem_competencia_vinculo,
        ine,
        tipo_equipe_id,
        cod_cbo,

        case
            when cod_cbo like '2251%' or cod_cbo = '2231F9' then 'MEDICO'
            when cod_cbo like '2235%' then 'ENFERMEIRO'
            when cod_cbo in ('322205', '322245', '322230', '322250') then 'TEC_AUX_ENFERMAGEM'
            when cod_cbo in ('515105', '322255') then 'ACS'
            when cod_cbo in ('223208', '223293') then 'CIRURGIAO_DENTISTA'
            when cod_cbo in ('322415', '322430') then 'AUX_BUCAL'
            when cod_cbo in ('322405', '322425') then 'TEC_BUCAL'
        end as categoria_composicao_sintetica,

        dt_entrada,
        dt_desligamento,
        equipe_ativa,
        vinculo_equipe_ativo,
        cg_horaamb

    from {{ ref("int_subpav_cnes_aps__profissionais_consolidacao") }}
    where possui_vinculo_equipe = 1
),

historico_profissional_cbo_sintetico_grupo as (
    select
        ca.data_particao,
        ca.ine,
        ca.tipo_equipe_id,
        ca.categoria,
        ca.cod_cbo_sintetico,
        pc.data_particao as data_particao_profissional,

        count(*) as total_linhas_grupo,

        countif(
            pc.dt_desligamento is null
            and pc.equipe_ativa = 1
            and pc.vinculo_equipe_ativo = 1
            and coalesce(pc.cg_horaamb, 0) >= 20
        ) as total_ativos_sem_desligamento,

        countif(pc.dt_desligamento is not null) as total_com_desligamento,

        max(pc.dt_desligamento) as max_dt_desligamento

    from categorias_ausentes ca

    left join profissionais_cbo_sintetico pc
        on pc.ine = ca.ine
        and pc.tipo_equipe_id = ca.tipo_equipe_id
        and pc.categoria_composicao_sintetica = ca.categoria
        and pc.data_particao <= ca.data_particao
        and pc.dt_entrada <= ca.dt_final_competencia

    group by
        ca.data_particao,
        ca.ine,
        ca.tipo_equipe_id,
        ca.categoria,
        ca.cod_cbo_sintetico,
        pc.data_particao
),

historico_profissional_cbo_sintetico as (
    select
        data_particao,
        ine,
        tipo_equipe_id,
        categoria,
        case
            when total_ativos_sem_desligamento > 0
                then null
            when total_com_desligamento > 0
                then max_dt_desligamento
            else null
        end as data_eqp_incompleta_desligamento
    from (
        select
            *,
            row_number() over (
                partition by
                    data_particao,
                    ine,
                    tipo_equipe_id,
                    categoria,
                    cod_cbo_sintetico
                order by
                    data_particao_profissional desc
            ) as ordem
        from historico_profissional_cbo_sintetico_grupo
        where data_particao_profissional is not null
    )
    where ordem = 1
),

perfil_competencia_fechamento as (
    select
        data_particao,
        0 as linhas_snapshot,
        0 as linhas_fechamento,
        0 as is_competencia_snapshot_incompleto
    from (
        select distinct data_particao
        from profissionais_cbo_sintetico
    )
),

historico_profissional_cbo_sintetico_status_grupo as (
    select
        ca.data_particao,
        ca.ine,
        ca.tipo_equipe_id,
        ca.categoria,
        ca.cod_cbo_sintetico,
        pc.data_particao as data_particao_profissional,

        count(*) as total_linhas_grupo,

        countif(
            pc.dt_desligamento is null
            and pc.equipe_ativa = 1
            and pc.vinculo_equipe_ativo = 1
            and coalesce(pc.cg_horaamb, 0) >= 20
        ) as total_ativos_sem_desligamento,

        countif(pc.dt_desligamento is not null) as total_com_desligamento

    from categorias_ausentes ca

    left join profissionais_cbo_sintetico pc
        on pc.ine = ca.ine
        and pc.tipo_equipe_id = ca.tipo_equipe_id
        and pc.categoria_composicao_sintetica = ca.categoria
        and pc.data_particao <= ca.data_particao
        and pc.dt_entrada <= ca.dt_final_competencia

    group by
        ca.data_particao,
        ca.ine,
        ca.tipo_equipe_id,
        ca.categoria,
        ca.cod_cbo_sintetico,
        pc.data_particao
),

historico_profissional_cbo_sintetico_status as (
    select
        data_particao,
        ine,
        tipo_equipe_id,
        categoria,
        cod_cbo_sintetico,
        data_particao_profissional as ultima_data_particao_profissional,
        total_linhas_grupo as ultimo_grupo_total_linhas,
        total_ativos_sem_desligamento as ultimo_grupo_ativos_sem_desligamento,
        total_com_desligamento as ultimo_grupo_com_desligamento
    from (
        select
            *,
            row_number() over (
                partition by
                    data_particao,
                    ine,
                    tipo_equipe_id,
                    categoria,
                    cod_cbo_sintetico
                order by data_particao_profissional desc
            ) as ordem
        from historico_profissional_cbo_sintetico_status_grupo
        where data_particao_profissional is not null
    )
    where ordem = 1
),

ausencias_sinteticas_com_lag as (
    select
        ca.*,

        lag(ca.competencia_id) over (
            partition by
                ca.ine,
                ca.tipo_equipe_id,
                ca.categoria,
                ca.cod_cbo_sintetico
            order by ca.competencia_id
        ) as competencia_id_ausencia_anterior,

        lag(ca.data_particao) over (
            partition by
                ca.ine,
                ca.tipo_equipe_id,
                ca.categoria,
                ca.cod_cbo_sintetico
            order by ca.competencia_id
        ) as data_particao_ausencia_anterior

    from categorias_ausentes ca
),

categoria_valida_no_intervalo as (
    select
        a.data_particao,
        a.ine,
        a.tipo_equipe_id,
        a.categoria,
        a.cod_cbo_sintetico,
        1 as possui_categoria_valida_no_intervalo

    from ausencias_sinteticas_com_lag a

    inner join status_categoria_mes s
        on s.ine = a.ine
        and s.tipo_equipe_id = a.tipo_equipe_id
        and s.categoria = a.categoria
        and s.data_particao > a.data_particao_ausencia_anterior
        and s.data_particao < a.data_particao
        and s.categoria_valida_composicao = 1

    where a.data_particao_ausencia_anterior is not null

    group by
        a.data_particao,
        a.ine,
        a.tipo_equipe_id,
        a.categoria,
        a.cod_cbo_sintetico
),

ausencias_sinteticas_com_original_intermediario as (
    select
        a.*,
        coalesce(o.possui_categoria_valida_no_intervalo, 0) as possui_categoria_valida_no_intervalo

    from ausencias_sinteticas_com_lag a

    left join categoria_valida_no_intervalo o
        on a.data_particao = o.data_particao
        and a.ine = o.ine
        and a.tipo_equipe_id = o.tipo_equipe_id
        and a.categoria = o.categoria
        and a.cod_cbo_sintetico = o.cod_cbo_sintetico
),

ausencias_sinteticas_com_ciclo as (
    select
        *,

        sum(
            case
                when competencia_id_ausencia_anterior is null
                    then 1
                when competencia_id - competencia_id_ausencia_anterior > 4
                    then 1
                when possui_categoria_valida_no_intervalo = 1
                    then 1
                else 0
            end
        ) over (
            partition by
                ine,
                tipo_equipe_id,
                categoria,
                cod_cbo_sintetico
            order by competencia_id
            rows between unbounded preceding and current row
        ) as ciclo_ausencia

    from ausencias_sinteticas_com_original_intermediario
),

data_inicio_ciclo_ausencia as (
    select
        *,

        min(
            coalesce(
                data_eqp_vacancia_categoria,
                data_eqp_incompleta_categoria,
                dt_final_competencia_anterior,
                data_particao
            )
        ) over (
            partition by
                ine,
                tipo_equipe_id,
                categoria,
                cod_cbo_sintetico,
                ciclo_ausencia
        ) as data_eqp_incompleta_por_ciclo

    from ausencias_sinteticas_com_ciclo
),

datas_incompletude_base_preliminar as (
    select
        dc.*,

        coalesce(pf.is_competencia_snapshot_incompleto, 0) as is_competencia_snapshot_incompleto,

        coalesce(hps.ultimo_grupo_ativos_sem_desligamento, 0) as ultimo_grupo_ativos_sem_desligamento,
        coalesce(hps.ultimo_grupo_com_desligamento, 0) as ultimo_grupo_com_desligamento,
        hps.ultima_data_particao_profissional,

        case
        when dc.data_eqp_incompleta_por_ciclo is not null
            then dc.data_eqp_incompleta_por_ciclo
        when hp.data_eqp_incompleta_desligamento is not null
            then hp.data_eqp_incompleta_desligamento
        else coalesce(dc.dt_final_competencia_anterior, dc.data_particao)
        end as data_eqp_incompleta_base_preliminar

    from data_inicio_ciclo_ausencia dc

    left join historico_profissional_cbo_sintetico hp
        on dc.data_particao = hp.data_particao
        and dc.ine = hp.ine
        and dc.tipo_equipe_id = hp.tipo_equipe_id
        and dc.categoria = hp.categoria

    left join historico_profissional_cbo_sintetico_status hps
        on dc.data_particao = hps.data_particao
        and dc.ine = hps.ine
        and dc.tipo_equipe_id = hps.tipo_equipe_id
        and dc.categoria = hps.categoria
        and dc.cod_cbo_sintetico = hps.cod_cbo_sintetico

    left join perfil_competencia_fechamento pf
        on dc.data_particao = pf.data_particao
),

datas_incompletude_base as (
    select
        * except(data_eqp_incompleta_base_preliminar),

        case
            when is_competencia_snapshot_incompleto = 1
                and ultimo_grupo_ativos_sem_desligamento > 0
                and date_diff(dt_final_competencia, data_eqp_incompleta_base_preliminar, day) between 61 and 70
                then coalesce(dt_final_competencia_anterior, data_particao)
            else data_eqp_incompleta_base_preliminar
        end as data_eqp_incompleta_base

    from datas_incompletude_base_preliminar
),

datas_incompletude as (
    select
        * except(data_eqp_incompleta_base),
        case
            when dt_ativacao is not null
                and data_eqp_incompleta_base < dt_ativacao
                then dt_ativacao
            else data_eqp_incompleta_base
        end as data_eqp_incompleta_calculada
    from datas_incompletude_base
),

linhas_sinteticas as (
    select
        d.data_particao,
        cast(extract(year from d.data_particao) as int64) as ano_particao,
        cast(extract(month from d.data_particao) as int64) as mes_particao,
        d.ap,
        d.ap_formatada,
        d.cnes,
        d.nome_unidade,
        d.ine,
        d.nm_referencia,
        d.tipo_equipe_id,
        d.classificacao_equipe,
        d.cod_cbo_sintetico as cod_cbo,
        d.categoria as categoria_profissional_composicao,
        d.categoria as categoria_profissional_vacancia_panorama,
        1 as is_cbo_composicao,
        1 as is_cbo_vacancia_panorama,
        0 as is_cbo_panorama,
        0 as total_profissionais,
        0 as total_40_horas,
        0 as total_20_horas,
        0 as total_desligados,
        date_diff(
            d.dt_final_competencia,
            d.data_eqp_incompleta_calculada,
            day
        ) as total_dias_desligados,
        case
            when date_diff(d.dt_final_competencia, d.data_eqp_incompleta_calculada, day) <= 60
                then d.data_eqp_incompleta_calculada
            else null
        end as data_eqp_vacancia,
        d.data_eqp_incompleta_calculada as data_eqp_incompleta,
        0 as inconsistente,
        'SINTETICA_CBO_AUSENTE' as origem_linha_qpe
    from datas_incompletude d
    where d.qtd_linhas_categoria = 0
),

qpe_base_padronizada as (
    select
        q.data_particao,
        safe_cast(q.ano_particao as int64) as ano_particao,
        safe_cast(q.mes_particao as int64) as mes_particao,
        q.ap,
        q.ap_formatada,
        q.cnes,
        q.nome_unidade,
        q.ine,
        e.nm_referencia,
        q.tipo_equipe_id,
        q.classificacao_equipe,
        q.cod_cbo,
        q.categoria_profissional_composicao,
        q.categoria_profissional_vacancia_panorama,
        q.is_cbo_composicao,
        q.is_cbo_vacancia_panorama,
        q.is_cbo_panorama,
        q.total_profissionais,
        q.total_40_horas,
        q.total_20_horas,
        q.total_desligados,
        q.total_dias_desligados,
        q.data_eqp_vacancia,
        q.data_eqp_incompleta,
        q.inconsistente,
        q.origem_linha_qpe
    from qpe_base q
    left join equipes e
        on q.data_particao = e.data_particao
        and q.ine = e.ine
        and q.tipo_equipe_id = e.tipo_equipe_id
),

final as (
    select * from qpe_base_padronizada

    union all

    select * from linhas_sinteticas
)

select *
from final
