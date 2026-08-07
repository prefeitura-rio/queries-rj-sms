{{
    config(
        schema = 'intermediario_plataforma_subpav',
        alias = 'cnes_aps__equipes_profissionais',
        materialized = "table",
        partition_by = {
            "field": "data_particao",
            "data_type": "date",
            "granularity": "month",
        },
        cluster_by = ["ine", "cpf", "cod_cbo"],
        tags = ["subpav", "cnes_aps"]
    )
}}

with fonte as (
    select
        _source_file,
        safe_cast(_loaded_at as timestamp) as loaded_at,
        safe_cast(data_particao as date) as data_particao,
        safe_cast(ano_particao as int64) as ano_particao,
        safe_cast(mes_particao as int64) as mes_particao,

        COD_MUN,
        COD_AREA,
        SEQ_EQUIPE,
        PROF_ID,
        UNIDADE_ID,
        COD_CBO,
        TP_SUS_NAO_SUS,
        IND_VINC,
        MICROAREA,
        DT_ENTRADA,
        DT_DESLIGAMENTO,
        CNES_OUTRAEQUIPE,
        COD_MUN_OUTRAEQUIPE,
        COD_AREA_OUTRAEQUIPE,
        PROF_ID_CH_COMPL,
        COD_CBO_CH_COMPL,
        FL_EQUIPEMINIMA,
        CO_MUN_ATUACAO,
        DATA_ATU,
        USUARIO,
        DT_ATUALIZACAO_ORIGEM,
        DT_CMTP_INICIO,
        DT_CMTP_FIM,
        NU_SEQ_PROCESSO

    from {{ ref("raw_gdb_cnes__lfces038") }}
),

profissionais_lookup as (
    select
        safe_cast(data_particao as date) as data_particao,
        nullif(cast(PROF_ID as string), '') as profissional_id_original,
        lpad(nullif(regexp_replace(cast(CPF_PROF as string), r'[^0-9]', ''), ''), 11, '0') as cpf,
        lpad(nullif(regexp_replace(cast(COD_CNS as string), r'[^0-9]', ''), ''), 15, '0') as cns,
        nullif(cast(NOME_PROF as string), '') as nome_profissional,
        safe_cast(nullif(cast(DATA_ATU as string), '') as date) as dt_atualiza_profissional,
        safe_cast(_loaded_at as timestamp) as loaded_at_profissional

    from {{ ref("raw_gdb_cnes__lfces018") }}

    qualify row_number() over (
        partition by
            safe_cast(data_particao as date),
            nullif(cast(PROF_ID as string), '')
        order by
            safe_cast(_loaded_at as timestamp) desc,
            safe_cast(nullif(cast(DATA_ATU as string), '') as date) desc
    ) = 1
),

profissionais_unidades_cbo as (
    select
        safe_cast(data_particao as date) as data_particao,
        nullif(cast(UNIDADE_ID as string), '') as unidade_id_original,
        nullif(cast(PROF_ID as string), '') as profissional_id_original,
        upper(nullif(cast(COD_CBO as string), '')) as cod_cbo,

        nullif(cast(IND_VINC as string), '') as vinculacao_id_original,
        nullif(cast(CONSELHOID as string), '') as conselho_id_original,
        nullif(cast(N_REGISTRO as string), '') as numero_registro,
        nullif(cast(SG_UF_CRM as string), '') as uf_registro,

        safe_cast(nullif(cast(CG_HORAAMB as string), '') as int64) as cg_horaamb,
        safe_cast(nullif(cast(CGHORAHOSP as string), '') as int64) as cg_horahosp,
        safe_cast(nullif(cast(CGHORAOUTR as string), '') as int64) as cg_horaoutr,

        nullif(cast(TP_PRECEPTOR as string), '') as tp_preceptor,
        nullif(cast(TP_RESIDENTE as string), '') as tp_residente,

        safe_cast(nullif(cast(DATA_ATU as string), '') as date) as dt_atualiza_vinculo_unidade,
        safe_cast(_loaded_at as timestamp) as loaded_at_vinculo_unidade

    from {{ ref("raw_gdb_cnes__lfces021") }}

    qualify row_number() over (
        partition by
            safe_cast(data_particao as date),
            nullif(cast(UNIDADE_ID as string), ''),
            nullif(cast(PROF_ID as string), ''),
            upper(nullif(cast(COD_CBO as string), ''))
        order by
            safe_cast(_loaded_at as timestamp) desc,
            safe_cast(nullif(cast(DATA_ATU as string), '') as date) desc,
            safe_cast(nullif(cast(CG_HORAAMB as string), '') as int64) desc
    ) = 1
),

profissionais_unidades_fallback as (
    select
        data_particao,
        unidade_id_original,
        profissional_id_original,

        array_agg(
            cod_cbo ignore nulls
            order by cg_horaamb desc, carga_horaria_total desc
            limit 1
        )[safe_offset(0)] as cod_cbo_fallback,

        max(cg_horaamb) as cg_horaamb,
        max(cg_horahosp) as cg_horahosp,
        max(cg_horaoutr) as cg_horaoutr,
        max(carga_horaria_total) as carga_horaria_total,

        array_agg(
            vinculacao_id_original ignore nulls
            order by cg_horaamb desc, carga_horaria_total desc
            limit 1
        )[safe_offset(0)] as vinculacao_id_original,

        array_agg(
            conselho_id_original ignore nulls
            order by cg_horaamb desc, carga_horaria_total desc
            limit 1
        )[safe_offset(0)] as conselho_id_original,

        array_agg(
            numero_registro ignore nulls
            order by cg_horaamb desc, carga_horaria_total desc
            limit 1
        )[safe_offset(0)] as numero_registro,

        array_agg(
            uf_registro ignore nulls
            order by cg_horaamb desc, carga_horaria_total desc
            limit 1
        )[safe_offset(0)] as uf_registro,

        max(tp_preceptor) as tp_preceptor,
        max(tp_residente) as tp_residente

    from (
        select
            *,
            coalesce(cg_horaamb, 0)
            + coalesce(cg_horahosp, 0)
            + coalesce(cg_horaoutr, 0) as carga_horaria_total
        from profissionais_unidades_cbo
    )

    group by
        data_particao,
        unidade_id_original,
        profissional_id_original
),

equipes as (
    select
        data_particao,
        ine,
        cnes,
        unidade_id_original,
        nome_unidade,
        ap,
        ap_formatada,
        is_municipio_rio,

        cod_mun,
        cod_area,
        seq_equipe,
        nm_referencia,

        tipo_equipe_id,
        tipo_equipe_descricao,
        grupo_equipe_id,
        subtipo_equipe_id,
        classificacao_equipe,
        classificacao_equipe_temporal,
        classificacao_equipe_painel,

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
        is_aps,

        tipo_unidade_sms,
        is_unidade_aps_panorama

    from {{ ref("int_subpav_cnes_aps__equipes") }}
),

extraido as (
    select
        
        nullif(cast(PROF_ID as string), '') as profissional_id_original,
        nullif(cast(PROF_ID_CH_COMPL as string), '') as profissional_id_ch_compl_original,
        nullif(cast(UNIDADE_ID as string), '') as unidade_id_original,
        nullif(cast(COD_MUN as string), '') as cod_mun,
        nullif(cast(COD_AREA as string), '') as cod_area,
        nullif(cast(SEQ_EQUIPE as string), '') as seq_equipe,

        upper(nullif(cast(COD_CBO as string), '')) as cod_cbo,
        upper(nullif(cast(COD_CBO_CH_COMPL as string), '')) as cod_cbo_ch_compl,

        
        safe_cast(nullif(cast(DT_ENTRADA as string), '') as date) as dt_entrada,
        safe_cast(nullif(cast(DT_DESLIGAMENTO as string), '') as date) as dt_desligamento,
        nullif(cast(FL_EQUIPEMINIMA as string), '') as fl_equipeminima_original,
        nullif(cast(MICROAREA as string), '') as microarea,

        
        nullif(cast(IND_VINC as string), '') as vinculacao_id_original_equipe,
        nullif(cast(TP_SUS_NAO_SUS as string), '') as tipo_sus_nao_sus,

        
        lpad(nullif(regexp_replace(cast(CNES_OUTRAEQUIPE as string), r'[^0-9]', ''), ''), 7, '0') as cnes_outra_equipe,
        nullif(cast(COD_MUN_OUTRAEQUIPE as string), '') as cod_mun_outra_equipe,
        nullif(cast(COD_AREA_OUTRAEQUIPE as string), '') as cod_area_outra_equipe,
        nullif(cast(CO_MUN_ATUACAO as string), '') as cod_municipio_atuacao,

        
        nullif(cast(USUARIO as string), '') as usuario_atualizacao,
        safe_cast(nullif(cast(DATA_ATU as string), '') as date) as dt_atualiza,
        safe_cast(nullif(cast(DT_ATUALIZACAO_ORIGEM as string), '') as date) as dt_atualizacao_origem,
        safe_cast(nullif(cast(DT_CMTP_INICIO as string), '') as date) as dt_cmtp_inicio,
        safe_cast(nullif(cast(DT_CMTP_FIM as string), '') as date) as dt_cmtp_fim,
        nullif(cast(NU_SEQ_PROCESSO as string), '') as nu_seq_processo,

        
        format_date('%Y-%m', data_particao) as competencia_mes,
        data_particao,
        ano_particao,
        mes_particao,
        loaded_at,
        _source_file

    from fonte
),

tratado as (
    select
        e.*,

        
        p.cpf,
        p.cns,
        p.nome_profissional,
        p.dt_atualiza_profissional,

        
        eq.ine,
        eq.cnes,
        eq.nome_unidade,
        eq.ap,
        eq.ap_formatada,
        eq.is_municipio_rio,
        eq.nm_referencia,

        eq.tipo_equipe_id,
        eq.tipo_equipe_descricao,
        eq.grupo_equipe_id,
        eq.subtipo_equipe_id,
        eq.classificacao_equipe,
        eq.classificacao_equipe_temporal,
        eq.classificacao_equipe_painel,

        
        eq.classificacao_equipe_temporal as classificacao_equipe_historica,

        eq.equipe_ativa,
        eq.is_esf,
        eq.is_esf_panorama_historico,
        eq.is_eacs,
        eq.is_eacs_panorama_historico,
        eq.is_esb,
        eq.is_ecr,
        eq.is_enasf,
        eq.is_eap,
        eq.is_eapp,
        eq.is_eapp_panorama,
        eq.is_emad,
        eq.is_emad_panorama,
        eq.is_emap,
        eq.is_equipe_aps_painel,
        eq.is_equipe_aps_cobertura,
        eq.is_equipe_aps_historico,
        eq.is_aps,

        
        eq.is_equipe_aps_historico as is_equipe_aps_cobertura_historica,
        eq.is_equipe_aps_painel as is_equipe_aps_cobertura_painel,
        eq.is_esb as is_equipe_saude_bucal,
        eq.is_enasf as is_equipe_emulti,
        eq.is_eapp_panorama as is_equipe_prisional,

        case
            when eq.is_emad_panorama = 1 or eq.is_emap = 1 then 1
            else 0
        end as is_equipe_ad,

        eq.tipo_unidade_sms,
        eq.is_unidade_aps_panorama,

        
        coalesce(pu.cg_horaamb, puf.cg_horaamb) as cg_horaamb,
        coalesce(pu.cg_horahosp, puf.cg_horahosp) as cg_horahosp,
        coalesce(pu.cg_horaoutr, puf.cg_horaoutr) as cg_horaoutr,

        coalesce(pu.cg_horaamb, puf.cg_horaamb, 0)
        + coalesce(pu.cg_horahosp, puf.cg_horahosp, 0)
        + coalesce(pu.cg_horaoutr, puf.cg_horaoutr, 0) as carga_horaria_total,

        case
            when coalesce(pu.cg_horaamb, puf.cg_horaamb) >= 40
                or coalesce(pu.cg_horaoutr, puf.cg_horaoutr) >= 40
                then '40'
            when (
                coalesce(pu.cg_horaamb, puf.cg_horaamb) >= 20
                and coalesce(pu.cg_horaamb, puf.cg_horaamb) < 40
            )
                or (
                    coalesce(pu.cg_horaoutr, puf.cg_horaoutr) >= 20
                    and coalesce(pu.cg_horaoutr, puf.cg_horaoutr) < 40
                )
                then '20'
            else null
        end as carga_horaria_classificacao,

        coalesce(
            pu.vinculacao_id_original,
            puf.vinculacao_id_original,
            e.vinculacao_id_original_equipe
        ) as vinculacao_id_original,

        coalesce(pu.conselho_id_original, puf.conselho_id_original) as conselho_id_original,
        coalesce(pu.numero_registro, puf.numero_registro) as numero_registro,
        coalesce(pu.uf_registro, puf.uf_registro) as uf_registro,
        coalesce(pu.tp_preceptor, puf.tp_preceptor) as tp_preceptor,
        coalesce(pu.tp_residente, puf.tp_residente) as tp_residente,

        case
            when pu.profissional_id_original is not null then 'EXATO_CBO'
            when puf.profissional_id_original is not null then 'FALLBACK_PROFISSIONAL_UNIDADE'
            else 'NAO_ENCONTRADO'
        end as tipo_enriquecimento_vinculo_unidade,

        concat(
            coalesce(p.cpf, ''),
            coalesce(eq.ine, ''),
            coalesce(e.cod_cbo, '')
        ) as chave_profissional_equipe_cbo,

        concat(
            coalesce(p.cpf, ''),
            coalesce(eq.cnes, ''),
            coalesce(e.cod_cbo, '')
        ) as chave_profissional_unidade_cbo,

        case
            when e.fl_equipeminima_original = '1' then 1
            when e.fl_equipeminima_original in ('0', '2') then 0
            else safe_cast(e.fl_equipeminima_original as int64)
        end as fl_equipeminima,

        case
            when e.dt_desligamento is not null then 1
            else 0
        end as possui_dt_desligamento,

        case
            when e.dt_desligamento is null then 1
            else 0
        end as vinculo_equipe_ativo,

        case when p.cpf is not null then 1 else 0 end as profissional_encontrado,
        case when eq.ine is not null then 1 else 0 end as equipe_encontrada,
        case when eq.cnes is not null then 1 else 0 end as unidade_equipe_encontrada,

        case
            when pu.profissional_id_original is not null
                or puf.profissional_id_original is not null
                then 1
            else 0
        end as vinculo_unidade_encontrado

    from extraido e

    left join profissionais_lookup p
        on e.data_particao = p.data_particao
        and e.profissional_id_original = p.profissional_id_original

    left join equipes eq
        on e.data_particao = eq.data_particao
        and e.unidade_id_original = eq.unidade_id_original
        and coalesce(e.cod_mun, '') = coalesce(eq.cod_mun, '')
        and coalesce(e.cod_area, '') = coalesce(eq.cod_area, '')
        and coalesce(e.seq_equipe, '') = coalesce(eq.seq_equipe, '')

    left join profissionais_unidades_cbo pu
        on e.data_particao = pu.data_particao
        and e.unidade_id_original = pu.unidade_id_original
        and e.profissional_id_original = pu.profissional_id_original
        and e.cod_cbo = pu.cod_cbo

    left join profissionais_unidades_fallback puf
        on e.data_particao = puf.data_particao
        and e.unidade_id_original = puf.unidade_id_original
        and e.profissional_id_original = puf.profissional_id_original
),

deduplicado as (
    select *
    from tratado
    where cpf is not null
        and ine is not null
        and cod_cbo is not null
    qualify row_number() over (
        partition by data_particao, cpf, ine, cod_cbo
        order by
            vinculo_equipe_ativo desc,
            fl_equipeminima desc,
            dt_entrada desc,
            loaded_at desc,
            dt_atualiza desc
    ) = 1
)

select
    cpf,
    cns,
    nome_profissional,
    profissional_id_original,
    profissional_id_ch_compl_original,

    ine,
    cnes,
    unidade_id_original,
    nome_unidade,
    ap,
    ap_formatada,
    is_municipio_rio,

    cod_mun,
    cod_area,
    seq_equipe,
    nm_referencia,

    cod_cbo,
    cod_cbo_ch_compl,
    vinculacao_id_original,
    vinculacao_id_original_equipe,
    tipo_sus_nao_sus,
    microarea,
    fl_equipeminima_original,
    fl_equipeminima,

    tipo_equipe_id,
    tipo_equipe_descricao,
    grupo_equipe_id,
    subtipo_equipe_id,
    classificacao_equipe,
    classificacao_equipe_temporal,
    classificacao_equipe_historica,
    classificacao_equipe_painel,

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
    is_equipe_aps_cobertura_historica,
    is_equipe_aps_cobertura_painel,
    is_equipe_saude_bucal,
    is_equipe_emulti,
    is_equipe_prisional,
    is_equipe_ad,
    is_aps,

    tipo_unidade_sms,
    is_unidade_aps_panorama,

    cg_horaamb,
    cg_horahosp,
    cg_horaoutr,
    carga_horaria_total,
    carga_horaria_classificacao,
    conselho_id_original,
    numero_registro,
    uf_registro,
    tp_preceptor,
    tp_residente,
    tipo_enriquecimento_vinculo_unidade,

    dt_entrada,
    dt_desligamento,
    possui_dt_desligamento,
    vinculo_equipe_ativo,

    cnes_outra_equipe,
    cod_mun_outra_equipe,
    cod_area_outra_equipe,
    cod_municipio_atuacao,

    chave_profissional_equipe_cbo,
    chave_profissional_unidade_cbo,
    profissional_encontrado,
    equipe_encontrada,
    unidade_equipe_encontrada,
    vinculo_unidade_encontrado,

    dt_atualiza,
    dt_atualiza_profissional,
    dt_atualizacao_origem,
    dt_cmtp_inicio,
    dt_cmtp_fim,
    usuario_atualizacao,
    nu_seq_processo,

    competencia_mes,
    data_particao,
    ano_particao,
    mes_particao,
    loaded_at,
    _source_file

from deduplicado
