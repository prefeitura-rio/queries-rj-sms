{{
    config(
        schema = 'intermediario_plataforma_subpav',
        alias = 'cnes_aps__qpe_com_cbo_ausente',
        materialized = "table",
        tags = ["subpav", "cnes_aps"]
    )
}}

with qpe_base as (
    select
        *,
        'ORIGINAL' as origem_linha_qpe
    from {{ ref("int_subpav_cnes_aps__quantitativos_profissionais_equipes") }}
),

equipes as (
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
        dt_ativacao
    from {{ ref("int_subpav_cnes_aps__equipes") }}
    where equipe_ativa = 1
      and tipo_equipe_id in (70, 71)
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
        data_particao,
        ine,
        tipo_equipe_id,
        categoria_composicao_sintetica as categoria,

        count(*) as qtd_linhas_categoria,
        sum(total_profissionais) as total_profissionais_categoria,
        sum(total_40_horas) as total_40h_categoria,
        sum(total_20_horas) as total_20h_categoria

    from qpe_categorizado
    where categoria_composicao_sintetica is not null
    group by
        data_particao,
        ine,
        tipo_equipe_id,
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

        m.categoria,
        m.cod_cbo_sintetico,
        m.grupo_tipo_equipe

    from equipes e
    inner join mapa_composicao m
        on e.tipo_equipe_id = m.tipo_equipe_id
),

categorias_ausentes as (
    select
        ce.*
    from categorias_esperadas ce
    left join presenca_categoria pc
        on ce.data_particao = pc.data_particao
        and ce.ine = pc.ine
        and ce.tipo_equipe_id = pc.tipo_equipe_id
        and ce.categoria = pc.categoria
    where pc.ine is null
       or coalesce(pc.total_profissionais_categoria, 0) = 0
),

ultima_presenca_categoria as (
    select
        ca.data_particao,
        ca.ine,
        ca.tipo_equipe_id,
        ca.categoria,

        max(pc.data_particao) as ultima_data_particao_com_categoria

    from categorias_ausentes ca
    left join presenca_categoria pc
        on ca.ine = pc.ine
        and ca.tipo_equipe_id = pc.tipo_equipe_id
        and ca.categoria = pc.categoria
        and pc.data_particao < ca.data_particao
        and coalesce(pc.total_profissionais_categoria, 0) > 0
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

        min(pc.data_particao) as primeira_data_particao_zero_categoria

    from categorias_ausentes ca
    left join ultima_presenca_categoria up
        on ca.data_particao = up.data_particao
        and ca.ine = up.ine
        and ca.tipo_equipe_id = up.tipo_equipe_id
        and ca.categoria = up.categoria
    left join presenca_categoria pc
        on ca.ine = pc.ine
        and ca.tipo_equipe_id = pc.tipo_equipe_id
        and ca.categoria = pc.categoria
        and pc.data_particao < ca.data_particao
        and coalesce(pc.total_profissionais_categoria, 0) = 0
        and (
            up.ultima_data_particao_com_categoria is null
            or pc.data_particao > up.ultima_data_particao_com_categoria
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

        max(ep.dt_desligamento) as ultima_dt_desligamento

    from categorias_ausentes ca
    left join {{ ref("int_subpav_cnes_aps__equipes_profissionais") }} ep
        on ca.ine = ep.ine
        and ca.cod_cbo_sintetico = ep.cod_cbo
        and ep.dt_desligamento is not null
        and ep.dt_desligamento <= last_day(ca.data_particao, month)
    group by
        ca.data_particao,
        ca.ine,
        ca.tipo_equipe_id,
        ca.categoria,
        ca.cod_cbo_sintetico
),

datas_incompletude as (
    select
        ca.*,

        coalesce(
            pz.primeira_data_particao_zero_categoria,
            (
                select max(dt)
                from unnest([
                    hd.ultima_dt_desligamento,
                    case
                        when up.ultima_data_particao_com_categoria is not null
                            then date_add(last_day(up.ultima_data_particao_com_categoria, month), interval 1 day)
                    end
                ]) as dt
                where dt is not null
            ),
            ca.data_particao
        ) as data_eqp_incompleta_calculada

    from categorias_ausentes ca
    left join ultima_presenca_categoria up
        on ca.data_particao = up.data_particao
        and ca.ine = up.ine
        and ca.tipo_equipe_id = up.tipo_equipe_id
        and ca.categoria = up.categoria
    left join primeira_competencia_zero_categoria pz
        on ca.data_particao = pz.data_particao
        and ca.ine = pz.ine
        and ca.tipo_equipe_id = pz.tipo_equipe_id
        and ca.categoria = pz.categoria
    left join historico_desligamento hd
        on ca.data_particao = hd.data_particao
        and ca.ine = hd.ine
        and ca.tipo_equipe_id = hd.tipo_equipe_id
        and ca.categoria = hd.categoria
),

linhas_sinteticas as (
    select
        d.data_particao,
        cast(extract(year from d.data_particao) as string) as ano_particao,
        lpad(cast(extract(month from d.data_particao) as string), 2, '0') as mes_particao,

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
            last_day(d.data_particao, month),
            d.data_eqp_incompleta_calculada,
            day
        ) as total_dias_desligados,

        case
            when date_diff(last_day(d.data_particao, month), d.data_eqp_incompleta_calculada, day) <= 60
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
        q.ano_particao,
        q.mes_particao,

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
        cast(null as date) as data_eqp_incompleta,

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
