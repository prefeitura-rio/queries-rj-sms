{{
    config(
        schema = 'intermediario_plataforma_subpav',
        alias = 'cnes_aps__profissionais_consolidacao',
        materialized = "table",
        tags = ["subpav", "cnes_aps"]
    )
}}

with competencias as (
    select
        competencia_id,
        competencia,
        data_particao,
        dt_final_competencia,
        dt_final_competencia_anterior,
        base_final
    from {{ ref("int_subpav_cnes_aps__competencias_legado") }}
    where dt_final_competencia is not null
),

profissionais as (
    select
        data_particao,
        cpf,
        sexo_id,
        sexo,
        sexo_id_original
    from {{ ref("int_subpav_cnes_aps__profissionais") }}
),

profissionais_unidades_base as (
    select
        c.data_particao,
        pu.data_particao as data_particao_origem_pu,
        'SNAPSHOT_MES' as origem_pu,
        1 as prioridade_pu,
        pu.* except(data_particao),

        concat(
            coalesce(pu.profissional_id_original, ''),
            '|',
            coalesce(pu.unidade_id_original, ''),
            '|',
            coalesce(pu.cod_cbo, '')
        ) as chave_profissional_unidade_cbo_base

    from {{ ref("int_subpav_cnes_aps__profissionais_unidades") }} pu

    inner join competencias c
        on c.data_particao = pu.data_particao
),

profissionais_unidades as (
    select *
    from profissionais_unidades_base

    qualify row_number() over (
        partition by
            data_particao,
            profissional_id_original,
            unidade_id_original,
            cod_cbo
        order by
            prioridade_pu desc,
            loaded_at desc,
            _source_file desc
    ) = 1
),

equipes_profissionais_base as (
    select
        c.data_particao,
        ep.data_particao as data_particao_origem_vinculo,
        'SNAPSHOT_MES' as origem_competencia_vinculo,
        1 as prioridade_origem_competencia_vinculo,

        case
            when ep.dt_desligamento is null then 1
            else 0
        end as vinculo_equipe_ativo_asof,

        ep.* except(data_particao)

    from {{ ref("int_subpav_cnes_aps__equipes_profissionais") }} ep

    inner join competencias c
        on c.data_particao = ep.data_particao

    where ep.equipe_ativa = 1
        and ep.cg_horaamb >= 20
),

equipes_profissionais_com_contagem as (
    select
        ep.*,

        count(*) over (
        partition by
            ep.data_particao,
            ep.profissional_id_original,
            ep.unidade_id_original,
            ep.cod_cbo
        ) as qtd_vinculos_mesma_chave,

        countif(ep.vinculo_equipe_ativo_asof = 1) over (
        partition by
            ep.data_particao,
            ep.profissional_id_original,
            ep.unidade_id_original,
            ep.cod_cbo
        ) as qtd_vinculos_ativos_mesma_chave

    from equipes_profissionais_base ep
),

equipes_profissionais as (
  select *
    from equipes_profissionais_com_contagem

    qualify row_number() over (
        partition by
        data_particao,
        profissional_id_original,
        unidade_id_original,
        cod_cbo,
        ine
        order by
        vinculo_equipe_ativo_asof desc,
        fl_equipeminima desc,
        dt_entrada desc,
        loaded_at desc,
        _source_file desc
    ) = 1
),

consolidado as (
    select
        coalesce(pu.data_particao, ep.data_particao) as data_particao,
        coalesce(pu.ano_particao, ep.ano_particao) as ano_particao,
        coalesce(pu.mes_particao, ep.mes_particao) as mes_particao,
        coalesce(pu.competencia_mes, ep.competencia_mes) as competencia_mes,

        c.competencia_id,
        c.competencia,
        c.dt_final_competencia,
        c.dt_final_competencia_anterior,
        c.base_final,

        coalesce(pu.cpf, ep.cpf) as cpf,
        coalesce(pu.cns, ep.cns) as cns,
        coalesce(pu.nome_profissional, ep.nome_profissional) as nome_profissional,
        coalesce(pu.profissional_id_original, ep.profissional_id_original) as profissional_id_original,

        p.sexo_id,
        p.sexo,
        p.sexo_id_original,

        coalesce(pu.cnes, ep.cnes) as cnes,
        coalesce(pu.unidade_id_original, ep.unidade_id_original) as unidade_id_original,
        coalesce(pu.nome_unidade, ep.nome_unidade) as nome_unidade,
        coalesce(pu.ap, ep.ap) as ap,
        coalesce(pu.ap_formatada, ep.ap_formatada) as ap_formatada,

        coalesce(pu.cod_cbo, ep.cod_cbo) as cod_cbo,
        coalesce(pu.vinculacao_id_original, ep.vinculacao_id_original) as vinculacao_id_original,

        coalesce(pu.tipo_sus_nao_sus, ep.tipo_sus_nao_sus) as tipo_sus_nao_sus,
        pu.detalhe_terceirizado_sih,
        pu.cnpj_detalhe_vinculo,

        coalesce(ep.cg_horaamb, pu.cg_horaamb) as cg_horaamb,
        coalesce(ep.cg_horahosp, pu.cg_horahosp) as cg_horahosp,
        coalesce(ep.cg_horaoutr, pu.cg_horaoutr) as cg_horaoutr,
        coalesce(ep.carga_horaria_total, pu.carga_horaria_total) as carga_horaria_total,
        coalesce(ep.carga_horaria_classificacao, pu.carga_horaria_classificacao) as carga_horaria_classificacao,

        coalesce(
            pu.possui_carga_horaria,
            case
                when coalesce(ep.cg_horaamb, ep.cg_horahosp, ep.cg_horaoutr) is not null then 1
                else 0
            end
        ) as possui_carga_horaria,

        coalesce(pu.conselho_id_original, ep.conselho_id_original) as conselho_id_original,
        coalesce(pu.numero_registro, ep.numero_registro) as numero_registro,
        coalesce(pu.uf_registro, ep.uf_registro) as uf_registro,

        coalesce(pu.tp_preceptor_original, ep.tp_preceptor) as tp_preceptor_original,

        coalesce(
        pu.tp_preceptor,
        case
            when ep.tp_preceptor = '1' then 1
            when ep.tp_preceptor = '2' then 0
            else null
        end
        ) as tp_preceptor,

        coalesce(pu.tp_residente_original, ep.tp_residente) as tp_residente_original,

        coalesce(
        pu.tp_residente,
        case
            when ep.tp_residente = '1' then 1
            when ep.tp_residente = '2' then 0
            else null
        end
        ) as tp_residente,

        coalesce(pu.tipo_unidade_sms, ep.tipo_unidade_sms) as tipo_unidade_sms,
        coalesce(pu.is_unidade_aps_panorama, ep.is_unidade_aps_panorama) as is_unidade_aps_panorama,
        pu.unidade_ativa,

        pu.status,
        pu.status_movimento,

        ep.ine,
        ep.seq_equipe,
        ep.cod_area,
        ep.nm_referencia,

        ep.tipo_equipe_id,
        ep.tipo_equipe_descricao,
        ep.grupo_equipe_id,
        ep.subtipo_equipe_id,
        ep.classificacao_equipe,
        ep.classificacao_equipe_temporal,
        ep.classificacao_equipe_historica,
        ep.classificacao_equipe_painel,

        ep.equipe_ativa,
        ep.fl_equipeminima,

        ep.is_esf,
        ep.is_esf_panorama_historico,
        ep.is_eacs,
        ep.is_eacs_panorama_historico,
        ep.is_esb,
        ep.is_ecr,
        ep.is_enasf,
        ep.is_eap,
        ep.is_eapp,
        ep.is_eapp_panorama,
        ep.is_emad,
        ep.is_emad_panorama,
        ep.is_emap,
        ep.is_equipe_aps_painel,
        ep.is_equipe_aps_cobertura,
        ep.is_equipe_aps_historico,
        ep.is_equipe_aps_cobertura_historica,
        ep.is_equipe_aps_cobertura_painel,
        ep.is_equipe_saude_bucal,
        ep.is_equipe_emulti,
        ep.is_equipe_prisional,
        ep.is_equipe_ad,
        ep.is_aps,

        ep.dt_entrada,
        ep.dt_desligamento,

        case
            when ep.dt_desligamento is not null
                then date_diff(c.dt_final_competencia, ep.dt_desligamento, day)
            else null
        end as dias_desligado,

        ep.possui_dt_desligamento,
        ep.vinculo_equipe_ativo,

        ep.dt_atualiza as dt_atualiza_equipe,
        pu.dt_atualiza as dt_atualiza_unidade,

        coalesce(ep.dt_atualiza, pu.dt_atualiza) as dt_atualiza,
        coalesce(ep.dt_atualiza_profissional, pu.dt_atualiza_profissional) as dt_atualiza_profissional,
        coalesce(ep.dt_atualizacao_origem, pu.dt_atualizacao_origem) as dt_atualizacao_origem,

        pu.dt_cmtp_inicio as pu_dt_cmtp_inicio,
        pu.dt_cmtp_fim as pu_dt_cmtp_fim,
        ep.dt_cmtp_inicio as ep_dt_cmtp_inicio,
        ep.dt_cmtp_fim as ep_dt_cmtp_fim,

        coalesce(ep.usuario_atualizacao, pu.usuario_atualizacao) as usuario_atualizacao,
        coalesce(ep.nu_seq_processo, pu.nu_seq_processo) as nu_seq_processo,

        coalesce(
            pu.chave_profissional_unidade_cbo_base,
            concat(
                coalesce(ep.profissional_id_original, ''),
                '|',
                coalesce(ep.unidade_id_original, ''),
                '|',
                coalesce(ep.cod_cbo, '')
            )
        ) as chave_profissional_unidade_cbo_base,

        ep.chave_profissional_equipe_cbo,
        ep.chave_profissional_unidade_cbo as ep_chave_profissional_unidade_cbo,
        pu.chave_profissional_unidade_cbo as pu_chave_profissional_unidade_cbo,

        ep.qtd_vinculos_mesma_chave,
        ep.qtd_vinculos_ativos_mesma_chave,

        case
            when ep.profissional_id_original is not null then 1
            else 0
        end as possui_vinculo_equipe,

        ep.origem_competencia_vinculo,
        ep.data_particao_origem_vinculo,
        case
                when coalesce(ep.qtd_vinculos_ativos_mesma_chave, 0) > 1 then 1
                else 0
        end as inconsistente,

        pu.loaded_at as pu_loaded_at,
        pu._source_file as pu_source_file,

        ep.loaded_at as ep_loaded_at,
        ep._source_file as ep_source_file,

        coalesce(ep.loaded_at, pu.loaded_at) as loaded_at,
        coalesce(ep._source_file, pu._source_file) as _source_file,

    from profissionais_unidades pu

    full outer join equipes_profissionais ep
        on ep.data_particao = pu.data_particao
        and ep.profissional_id_original = pu.profissional_id_original
        and ep.unidade_id_original = pu.unidade_id_original
        and ep.cod_cbo = pu.cod_cbo

    inner join competencias c
        on c.data_particao = coalesce(pu.data_particao, ep.data_particao)

    left join profissionais p
        on p.data_particao = c.data_particao
        and p.cpf = coalesce(pu.cpf, ep.cpf)
)

select *
from consolidado
