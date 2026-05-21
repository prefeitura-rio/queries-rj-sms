{{
    config(
        schema = 'intermediario_plataforma_subpav',
        alias = 'cnes_aps__equipes_profissionais',
        materialized = "table",
        tags = ["subpav", "cnes_aps"]
    )
}}

with fonte as (
    select
        json,
        _source_file,
        safe_cast(_loaded_at as timestamp) as loaded_at,
        safe_cast(data_particao as date) as data_particao,
        ano_particao,
        mes_particao
    from {{ source("brutos_gdb_cnes_staging", "LFCES038") }}
),

profissionais_lookup as (
    select
        safe_cast(data_particao as date) as data_particao,
        nullif(json_value(json, '$.PROF_ID'), '') as profissional_id_original,
        lpad(nullif(json_value(json, '$.CPF_PROF'), ''), 11, '0') as cpf,
        lpad(nullif(json_value(json, '$.COD_CNS'), ''), 15, '0') as cns,
        nullif(json_value(json, '$.NOME_PROF'), '') as nome_profissional,
        safe_cast(nullif(json_value(json, '$.DATA_ATU'), '') as date) as dt_atualiza_profissional,
        safe_cast(_loaded_at as timestamp) as loaded_at_profissional
    from {{ source("brutos_gdb_cnes_staging", "LFCES018") }}
    qualify row_number() over (
        partition by
            safe_cast(data_particao as date),
            nullif(json_value(json, '$.PROF_ID'), '')
        order by
            safe_cast(_loaded_at as timestamp) desc,
            safe_cast(nullif(json_value(json, '$.DATA_ATU'), '') as date) desc
    ) = 1
),

equipes_lookup as (
    select
        safe_cast(data_particao as date) as data_particao,
        nullif(json_value(json, '$.UNIDADE_ID'), '') as unidade_id_original,
        nullif(json_value(json, '$.COD_MUN'), '') as cod_mun,
        nullif(json_value(json, '$.COD_AREA'), '') as cod_area,
        nullif(json_value(json, '$.SEQ_EQUIPE'), '') as seq_equipe,
        lpad(nullif(json_value(json, '$.CO_EQUIPE'), ''), 10, '0') as ine,
        safe_cast(nullif(json_value(json, '$.TP_EQUIPE'), '') as int64) as tipo_equipe_id,
        safe_cast(nullif(json_value(json, '$.DATA_ATU'), '') as date) as dt_atualiza_equipe,
        safe_cast(_loaded_at as timestamp) as loaded_at_equipe
    from {{ source("brutos_gdb_cnes_staging", "LFCES037") }}
    qualify row_number() over (
        partition by
            safe_cast(data_particao as date),
            nullif(json_value(json, '$.UNIDADE_ID'), ''),
            nullif(json_value(json, '$.COD_MUN'), ''),
            nullif(json_value(json, '$.COD_AREA'), ''),
            nullif(json_value(json, '$.SEQ_EQUIPE'), '')
        order by
            safe_cast(_loaded_at as timestamp) desc,
            safe_cast(nullif(json_value(json, '$.DATA_ATU'), '') as date) desc
    ) = 1
),

equipes as (
    select
        data_particao,
        ine,
        cnes,
        ap,
        ap_formatada,
        nome_unidade,
        tipo_equipe_id,
        classificacao_equipe,
        classificacao_equipe_historica,
        classificacao_equipe_painel,
        equipe_ativa,
        is_equipe_aps_cobertura,
        is_equipe_aps_cobertura_historica,
        is_equipe_aps_cobertura_painel,
        is_equipe_saude_bucal,
        is_equipe_emulti,
        is_equipe_prisional,
        is_equipe_ad,
        is_aps
    from {{ ref("int_subpav_cnes_aps__equipes") }}
),

profissionais_unidades as (
    select
        data_particao,
        cpf,
        cnes,
        cod_cbo,
        cg_horaamb,
        cg_horahosp,
        cg_horaoutr,
        carga_horaria_total,
        carga_horaria_classificacao,
        vinculacao_id_original,
        conselho_id_original,
        numero_registro,
        uf_registro,
        tp_preceptor,
        tp_residente
    from {{ ref("int_subpav_cnes_aps__profissionais_unidades") }}
),

profissionais_unidades_fallback as (
    select
        data_particao,
        cpf,
        cnes,

        array_agg(cod_cbo order by cg_horaamb desc, carga_horaria_total desc limit 1)[offset(0)] as cod_cbo_fallback,
        max(cg_horaamb) as cg_horaamb,
        max(cg_horahosp) as cg_horahosp,
        max(cg_horaoutr) as cg_horaoutr,
        max(carga_horaria_total) as carga_horaria_total,

        array_agg(carga_horaria_classificacao order by cg_horaamb desc, carga_horaria_total desc limit 1)[offset(0)] as carga_horaria_classificacao,
        array_agg(vinculacao_id_original order by cg_horaamb desc, carga_horaria_total desc limit 1)[offset(0)] as vinculacao_id_original,
        array_agg(conselho_id_original order by cg_horaamb desc, carga_horaria_total desc limit 1)[offset(0)] as conselho_id_original,
        array_agg(numero_registro order by cg_horaamb desc, carga_horaria_total desc limit 1)[offset(0)] as numero_registro,
        array_agg(uf_registro order by cg_horaamb desc, carga_horaria_total desc limit 1)[offset(0)] as uf_registro,
        max(tp_preceptor) as tp_preceptor,
        max(tp_residente) as tp_residente
    from {{ ref("int_subpav_cnes_aps__profissionais_unidades") }}
    group by
        data_particao,
        cpf,
        cnes
),

extraido as (
    select
        -- metadados da carga
        data_particao,
        ano_particao,
        mes_particao,
        loaded_at,
        _source_file,

        -- chaves técnicas originais
        nullif(json_value(json, '$.PROF_ID'), '') as profissional_id_original,
        nullif(json_value(json, '$.PROF_ID_CH_COMPL'), '') as profissional_id_ch_compl_original,
        nullif(json_value(json, '$.UNIDADE_ID'), '') as unidade_id_original,
        nullif(json_value(json, '$.COD_MUN'), '') as cod_mun,
        nullif(json_value(json, '$.COD_AREA'), '') as cod_area,
        nullif(json_value(json, '$.SEQ_EQUIPE'), '') as seq_equipe,
        nullif(json_value(json, '$.COD_CBO'), '') as cod_cbo,
        nullif(json_value(json, '$.COD_CBO_CH_COMPL'), '') as cod_cbo_ch_compl,

        -- vínculo na equipe
        safe_cast(nullif(json_value(json, '$.DT_ENTRADA'), '') as date) as dt_entrada,
        safe_cast(nullif(json_value(json, '$.DT_DESLIGAMENTO'), '') as date) as dt_desligamento,
        nullif(json_value(json, '$.FL_EQUIPEMINIMA'), '') as fl_equipeminima_original,
        nullif(json_value(json, '$.MICROAREA'), '') as microarea,

        -- vínculo / SUS
        nullif(json_value(json, '$.IND_VINC'), '') as vinculacao_id_original,
        nullif(json_value(json, '$.TP_SUS_NAO_SUS'), '') as tipo_sus_nao_sus,

        -- outra equipe / atuação
        nullif(json_value(json, '$.CNES_OUTRAEQUIPE'), '') as cnes_outra_equipe,
        nullif(json_value(json, '$.COD_MUN_OUTRAEQUIPE'), '') as cod_mun_outra_equipe,
        nullif(json_value(json, '$.COD_AREA_OUTRAEQUIPE'), '') as cod_area_outra_equipe,
        nullif(json_value(json, '$.CO_MUN_ATUACAO'), '') as cod_municipio_atuacao,

        -- atualização
        safe_cast(nullif(json_value(json, '$.DATA_ATU'), '') as date) as dt_atualiza

    from fonte
),

tratado as (
    select
        e.*,

        -- profissional resolvido pela LFCES018
        p.cpf,
        p.cns,
        p.nome_profissional,

        -- equipe resolvida pela LFCES037
        el.ine,

        -- atributos da equipe/unidade já tratados
        eq.cnes,
        eq.ap,
        eq.ap_formatada,
        eq.nome_unidade,
        eq.tipo_equipe_id,
        eq.classificacao_equipe,
        eq.classificacao_equipe_historica,
        eq.classificacao_equipe_painel,
        eq.equipe_ativa,
        eq.is_equipe_aps_cobertura,
        eq.is_equipe_aps_cobertura_historica,
        eq.is_equipe_aps_cobertura_painel,
        eq.is_equipe_saude_bucal,
        eq.is_equipe_emulti,
        eq.is_equipe_prisional,
        eq.is_equipe_ad,
        eq.is_aps,

        -- dados complementares do vínculo profissional-unidade
        coalesce(pu.cg_horaamb, puf.cg_horaamb) as cg_horaamb,
        coalesce(pu.cg_horahosp, puf.cg_horahosp) as cg_horahosp,
        coalesce(pu.cg_horaoutr, puf.cg_horaoutr) as cg_horaoutr,
        coalesce(pu.carga_horaria_total, puf.carga_horaria_total) as carga_horaria_total,
        coalesce(pu.carga_horaria_classificacao, puf.carga_horaria_classificacao) as carga_horaria_classificacao,
        coalesce(pu.conselho_id_original, puf.conselho_id_original) as conselho_id_original,
        coalesce(pu.numero_registro, puf.numero_registro) as numero_registro,
        coalesce(pu.uf_registro, puf.uf_registro) as uf_registro,
        coalesce(pu.tp_preceptor, puf.tp_preceptor) as tp_preceptor,
        coalesce(pu.tp_residente, puf.tp_residente) as tp_residente,

        case
            when pu.cpf is not null then 'EXATO_CBO'
            when puf.cpf is not null then 'FALLBACK_PROFISSIONAL_UNIDADE'
            else 'NAO_ENCONTRADO'
        end as tipo_enriquecimento_vinculo_unidade,

        concat(
            coalesce(p.cpf, ''),
            coalesce(el.ine, ''),
            coalesce(e.cod_cbo, '')
        ) as chave_profissional_equipe_cbo,

        concat(
            coalesce(p.cpf, ''),
            coalesce(eq.cnes, ''),
            coalesce(e.cod_cbo, '')
        ) as chave_profissional_unidade_cbo,

        case
            when e.fl_equipeminima_original = '1' then 1
            when e.fl_equipeminima_original = '2' then 0
            when e.fl_equipeminima_original = '0' then 0
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
        case when el.ine is not null then 1 else 0 end as equipe_encontrada,
        case when eq.cnes is not null then 1 else 0 end as unidade_equipe_encontrada,
        case
            when pu.cpf is not null or puf.cpf is not null then 1
            else 0
        end as vinculo_unidade_encontrado

    from extraido e
    left join profissionais_lookup p
        on e.data_particao = p.data_particao
        and e.profissional_id_original = p.profissional_id_original

    left join equipes_lookup el
        on e.data_particao = el.data_particao
        and e.unidade_id_original = el.unidade_id_original
        and coalesce(e.cod_mun, '') = coalesce(el.cod_mun, '')
        and coalesce(e.cod_area, '') = coalesce(el.cod_area, '')
        and coalesce(e.seq_equipe, '') = coalesce(el.seq_equipe, '')

    left join equipes eq
        on e.data_particao = eq.data_particao
        and el.ine = eq.ine

    left join profissionais_unidades pu
        on e.data_particao = pu.data_particao
        and p.cpf = pu.cpf
        and eq.cnes = pu.cnes
        and e.cod_cbo = pu.cod_cbo

    left join profissionais_unidades_fallback puf
        on e.data_particao = puf.data_particao
        and p.cpf = puf.cpf
        and eq.cnes = puf.cnes
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

select *
from deduplicado
