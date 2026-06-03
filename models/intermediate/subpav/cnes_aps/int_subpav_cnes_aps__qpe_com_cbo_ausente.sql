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
        *,
        'ORIGINAL' as origem_linha_qpe
    from {{ ref("int_subpav_cnes_aps__quantitativos_profissionais_equipes") }}
),

competencias_legado as (
    select
        data_particao,
        competencia,
        dt_final_competencia,

        lag(dt_final_competencia) over (
            order by data_particao
        ) as dt_final_competencia_anterior

    from {{ ref("int_subpav_cnes_aps__competencias_legado") }}
    where dt_final_competencia is not null
),

janela_competencias_legado as (
    select
        atual.data_particao as data_particao_atual,
        hist.data_particao as data_particao_historica,
        hist.competencia,
        hist.dt_final_competencia,

        row_number() over (
            partition by atual.data_particao
            order by hist.dt_final_competencia desc
        ) as ordem_historico

    from competencias_legado atual
    inner join competencias_legado hist
        on hist.dt_final_competencia <= atual.dt_final_competencia
),

competencias_historico_4 as (
    select
        data_particao_atual,
        data_particao_historica
    from janela_competencias_legado
    where ordem_historico <= 4
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

        cl.dt_final_competencia,
        cl.dt_final_competencia_anterior

    from {{ ref("int_subpav_cnes_aps__equipes") }} e
    left join competencias_legado cl
        on e.data_particao = cl.data_particao
    where e.equipe_ativa = 1
        and e.tipo_equipe_id in (70, 71)
),

mapa_composicao as (
    -- eSF
    select 70 as tipo_equipe_id, 'MEDICO' as categoria, '225142' as cod_cbo_sintetico, 'ESF' as grupo_tipo_equipe union all
    select 70, 'ENFERMEIRO', '223565', 'ESF' union all
    select 70, 'TEC_AUX_ENFERMAGEM', '322250', 'ESF' union all
    select 70, 'ACS', '515105', 'ESF' union all

    -- ESB
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
        sum(coalesce(q.total_20_horas, 0)) as total_20h_categoria

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
        ce.dt_final_competencia,
        ce.dt_final_competencia_anterior,
        ce.categoria,
        ce.cod_cbo_sintetico,
        ce.grupo_tipo_equipe,

        coalesce(pc.qtd_linhas_categoria, 0) as qtd_linhas_categoria,
        coalesce(pc.total_profissionais_categoria, 0) as total_profissionais_categoria,
        coalesce(pc.total_40h_categoria, 0) as total_40h_categoria,
        coalesce(pc.total_20h_categoria, 0) as total_20h_categoria

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
        dt_final_competencia,
        dt_final_competencia_anterior,
        categoria,
        cod_cbo_sintetico,
        grupo_tipo_equipe,

        total_profissionais_categoria,
        total_40h_categoria,
        total_20h_categoria,
        categoria_valida_composicao,

        1 as is_categoria_totalmente_ausente

    from status_categoria_mes
    where total_profissionais_categoria = 0
),

ultima_presenca_valida_categoria as (
    select
        ca.data_particao,
        ca.ine,
        ca.tipo_equipe_id,
        ca.categoria,

        max(scm.data_particao) as ultima_data_particao_com_categoria_valida

    from categorias_ausentes ca

    left join competencias_historico_4 ch
        on ca.data_particao = ch.data_particao_atual

    left join status_categoria_mes scm
        on ca.ine = scm.ine
        and ca.tipo_equipe_id = scm.tipo_equipe_id
        and ca.categoria = scm.categoria
        and scm.data_particao = ch.data_particao_historica
        and scm.data_particao < ca.data_particao
        and scm.categoria_valida_composicao = 1

    group by
        ca.data_particao,
        ca.ine,
        ca.tipo_equipe_id,
        ca.categoria
),

ultima_presenca_qualquer_categoria as (
    select
        ca.data_particao,
        ca.ine,
        ca.tipo_equipe_id,
        ca.categoria,

        max(scm.data_particao) as ultima_data_particao_com_qualquer_profissional_categoria

    from categorias_ausentes ca

    left join competencias_historico_4 ch
        on ca.data_particao = ch.data_particao_atual

    left join status_categoria_mes scm
        on ca.ine = scm.ine
        and ca.tipo_equipe_id = scm.tipo_equipe_id
        and ca.categoria = scm.categoria
        and scm.data_particao = ch.data_particao_historica
        and scm.data_particao < ca.data_particao
        and scm.total_profissionais_categoria > 0

    group by
        ca.data_particao,
        ca.ine,
        ca.tipo_equipe_id,
        ca.categoria
),

primeira_competencia_zero_categoria as (
    select
        ca.data_particao,
        ca.ine,
        ca.tipo_equipe_id,
        ca.categoria,

        max(upv.ultima_data_particao_com_categoria_valida) as ultima_data_particao_com_categoria_valida,
        max(upq.ultima_data_particao_com_qualquer_profissional_categoria) as ultima_data_particao_com_qualquer_profissional_categoria,

        array_agg(
            scm.data_particao ignore nulls
            order by scm.data_particao
            limit 1
        )[safe_offset(0)] as primeira_data_particao_zero_categoria,

        array_agg(
            case
                -- Se já houve qualquer profissional da categoria antes da ausência,
                -- o legado começa a contagem na data de fechamento da competência anterior
                -- à primeira competência zerada.
                when upq.ultima_data_particao_com_qualquer_profissional_categoria is not null
                    then coalesce(
                        scm.dt_final_competencia_anterior,
                        scm.data_particao
                    )

                -- Se nunca houve profissional da categoria no histórico recente,
                -- começa a contar da data final da própria primeira ausência,
                -- evitando inflar equipes historicamente sem a categoria.
                else coalesce(
                    scm.dt_final_competencia,
                    scm.data_particao
                )
            end ignore nulls
            order by scm.data_particao
            limit 1
        )[safe_offset(0)] as data_eqp_incompleta_primeira_ausencia

    from categorias_ausentes ca

    left join ultima_presenca_valida_categoria upv
        on ca.data_particao = upv.data_particao
        and ca.ine = upv.ine
        and ca.tipo_equipe_id = upv.tipo_equipe_id
        and ca.categoria = upv.categoria

    left join ultima_presenca_qualquer_categoria upq
        on ca.data_particao = upq.data_particao
        and ca.ine = upq.ine
        and ca.tipo_equipe_id = upq.tipo_equipe_id
        and ca.categoria = upq.categoria

    left join competencias_historico_4 ch
        on ca.data_particao = ch.data_particao_atual

    left join status_categoria_mes scm
        on ca.ine = scm.ine
        and ca.tipo_equipe_id = scm.tipo_equipe_id
        and ca.categoria = scm.categoria
        and scm.data_particao = ch.data_particao_historica
        and scm.data_particao <= ca.data_particao
        and scm.total_profissionais_categoria = 0
        and (
            upq.ultima_data_particao_com_qualquer_profissional_categoria is null
            or scm.data_particao > upq.ultima_data_particao_com_qualquer_profissional_categoria
        )

    group by
        ca.data_particao,
        ca.ine,
        ca.tipo_equipe_id,
        ca.categoria
),

historico_desligamento as (
    select
        ca.data_particao,
        ca.ine,
        ca.tipo_equipe_id,
        ca.categoria,
        ca.cod_cbo_sintetico,
        ca.dt_final_competencia,
        max(pc.dt_desligamento) as ultima_dt_desligamento

    from categorias_ausentes ca
    left join competencias_historico_4 ch
        on ca.data_particao = ch.data_particao_atual
    left join (
        select
            data_particao,
            ine,
            tipo_equipe_id,
            cod_cbo,
            dt_entrada,
            dt_desligamento,

            case
                when upper(cod_cbo) like '2251%' or upper(cod_cbo) = '2231F9' then 'MEDICO'
                when upper(cod_cbo) like '2235%' then 'ENFERMEIRO'
                when upper(cod_cbo) in ('322205', '322245', '322230', '322250') then 'TEC_AUX_ENFERMAGEM'
                when upper(cod_cbo) in ('515105', '322255') then 'ACS'

                when upper(cod_cbo) in ('223208', '223293') then 'CIRURGIAO_DENTISTA'
                when upper(cod_cbo) in ('322415', '322430') then 'AUX_BUCAL'
                when upper(cod_cbo) in ('322405', '322425') then 'TEC_BUCAL'
            end as categoria

        from {{ ref("int_subpav_cnes_aps__profissionais_consolidacao") }}
        where dt_desligamento is not null
            and possui_vinculo_equipe = 1
    ) pc
        on ca.ine = pc.ine
        and ca.tipo_equipe_id = pc.tipo_equipe_id
        and ca.categoria = pc.categoria
        and pc.data_particao = ch.data_particao_historica
        and pc.dt_desligamento <= ca.dt_final_competencia

    group by
        ca.data_particao,
        ca.ine,
        ca.tipo_equipe_id,
        ca.categoria,
        ca.cod_cbo_sintetico,
        ca.dt_final_competencia
),

retorno_apos_desligamento as (
    select
        hd.data_particao,
        hd.ine,
        hd.tipo_equipe_id,
        hd.categoria,
        hd.cod_cbo_sintetico,
        hd.ultima_dt_desligamento,
        hd.dt_final_competencia,
        countif(
            pc.dt_entrada > hd.ultima_dt_desligamento
            and pc.dt_entrada <= hd.dt_final_competencia
            and pc.dt_desligamento is null
        ) as qtd_retorno_apos_desligamento

    from historico_desligamento hd
    left join (
        select
            data_particao,
            ine,
            tipo_equipe_id,
            cod_cbo,
            dt_entrada,
            dt_desligamento,

            case
                when upper(cod_cbo) like '2251%' or upper(cod_cbo) = '2231F9' then 'MEDICO'
                when upper(cod_cbo) like '2235%' then 'ENFERMEIRO'
                when upper(cod_cbo) in ('322205', '322245', '322230', '322250') then 'TEC_AUX_ENFERMAGEM'
                when upper(cod_cbo) in ('515105', '322255') then 'ACS'
                when upper(cod_cbo) in ('223208', '223293') then 'CIRURGIAO_DENTISTA'
                when upper(cod_cbo) in ('322415', '322430') then 'AUX_BUCAL'
                when upper(cod_cbo) in ('322405', '322425') then 'TEC_BUCAL'
            end as categoria

        from {{ ref("int_subpav_cnes_aps__profissionais_consolidacao") }}
        where possui_vinculo_equipe = 1
    ) pc
        on hd.ine = pc.ine
        and hd.tipo_equipe_id = pc.tipo_equipe_id
        and hd.categoria = pc.categoria
        and pc.data_particao <= hd.data_particao

    group by
        hd.data_particao,
        hd.ine,
        hd.tipo_equipe_id,
        hd.categoria,
        hd.cod_cbo_sintetico,
        hd.ultima_dt_desligamento,
        hd.dt_final_competencia
),

historico_qpe_vacancia as (
    select
        ca.data_particao,
        ca.ine,
        ca.tipo_equipe_id,
        ca.categoria,

        min(coalesce(q.data_eqp_incompleta, q.data_eqp_vacancia)) as data_eqp_incompleta_qpe

    from categorias_ausentes ca

    left join ultima_presenca_qualquer_categoria upq
        on ca.data_particao = upq.data_particao
        and ca.ine = upq.ine
        and ca.tipo_equipe_id = upq.tipo_equipe_id
        and ca.categoria = upq.categoria

    left join competencias_historico_4 ch
        on ca.data_particao = ch.data_particao_atual

    left join qpe_categorizado q
        on ca.ine = q.ine
        and ca.tipo_equipe_id = q.tipo_equipe_id
        and ca.categoria = q.categoria_composicao_sintetica
        and q.data_particao = ch.data_particao_historica
        and q.data_particao <= ca.data_particao
        and coalesce(q.total_profissionais, 0) = 0
        and coalesce(q.data_eqp_incompleta, q.data_eqp_vacancia) is not null
        and (
            upq.ultima_data_particao_com_qualquer_profissional_categoria is null
            or q.data_particao > upq.ultima_data_particao_com_qualquer_profissional_categoria
        )

    group by
        ca.data_particao,
        ca.ine,
        ca.tipo_equipe_id,
        ca.categoria
),

datas_incompletude_base as (
    select
        ca.*,

        case
            when hq.data_eqp_incompleta_qpe is not null
                then hq.data_eqp_incompleta_qpe

            when r.ultima_dt_desligamento is not null
                and coalesce(r.qtd_retorno_apos_desligamento, 0) = 0
                then r.ultima_dt_desligamento

            when ca.is_categoria_totalmente_ausente = 1
                and pz.data_eqp_incompleta_primeira_ausencia is not null
                then pz.data_eqp_incompleta_primeira_ausencia

            when ca.is_categoria_totalmente_ausente = 1
                then coalesce(ca.dt_final_competencia, ca.data_particao)

            else ca.dt_final_competencia
        end as data_eqp_incompleta_base

    from categorias_ausentes ca

    left join primeira_competencia_zero_categoria pz
        on ca.data_particao = pz.data_particao
        and ca.ine = pz.ine
        and ca.tipo_equipe_id = pz.tipo_equipe_id
        and ca.categoria = pz.categoria

    left join retorno_apos_desligamento r
        on ca.data_particao = r.data_particao
        and ca.ine = r.ine
        and ca.tipo_equipe_id = r.tipo_equipe_id
        and ca.categoria = r.categoria

    left join historico_qpe_vacancia hq
        on ca.data_particao = hq.data_particao
        and ca.ine = hq.ine
        and ca.tipo_equipe_id = hq.tipo_equipe_id
        and ca.categoria = hq.categoria
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

        d.categoria as categoria_profissional_composicao_legado,

        case
            when d.grupo_tipo_equipe = 'ESF' then d.categoria
            when d.grupo_tipo_equipe = 'ESB' then d.categoria
        end as categoria_profissional_vacancia_panorama,

        1 as is_cbo_composicao_legado,
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

        q.categoria_profissional_composicao_legado,
        q.categoria_profissional_vacancia_panorama,

        q.is_cbo_composicao_legado,
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
