{{
    config(
        schema = 'intermediario_plataforma_subpav',
        alias = 'cnes_aps__profissionais_consolidacao',
        materialized = "table",
        partition_by = {
            "field": "data_particao",
            "data_type": "date",
            "granularity": "month",
        },
        cluster_by = ["cpf", "cnes", "ine", "cod_cbo"],
        tags = ["subpav", "cnes_aps"]
    )
}}

with competencias_legado as (
    select
        data_particao,
        competencia,
        dt_final_competencia,
        dt_final_competencia_anterior

    from {{ ref("int_subpav_cnes_aps__competencias_legado") }}
    where dt_final_competencia is not null
),

profissionais_unidades as (
    select
        pu.data_particao,
        pu.ano_particao,
        pu.mes_particao,

        -- Auditoria: nesta versão controlada, a base de unidade/CBO permanece
        -- ancorada no snapshot da própria competência. Não projetamos PU do
        -- mês seguinte porque isso melhorou alguns casos pontuais, mas piorou
        -- o histórico em várias competências.
        pu.data_particao as data_particao_origem_cnes,

        cl.dt_final_competencia,
        cl.dt_final_competencia_anterior,

        pu.loaded_at,
        pu._source_file,

        pu.profissional_id_original,
        pu.unidade_id_original,

        pu.cpf,
        pu.cns,
        pu.nome_profissional,

        pu.cnes,
        pu.ap,
        pu.ap_formatada,
        pu.nome_unidade,
        pu.is_municipio_rio,

        pu.cod_cbo,
        pu.vinculacao_id_original,
        pu.tipo_sus_nao_sus,
        pu.detalhe_terceirizado_sih,
        pu.cnpj_detalhe_vinculo,

        pu.cg_horaamb,
        pu.cg_horahosp,
        pu.cg_horaoutr,
        pu.carga_horaria_total,
        pu.carga_horaria_classificacao,
        pu.possui_carga_horaria,

        pu.conselho_id_original,
        pu.numero_registro,
        pu.uf_registro,

        pu.tp_preceptor,
        pu.tp_residente,

        pu.tipo_unidade_sms,
        pu.is_unidade_aps_panorama,
        pu.unidade_ativa,

        pu.profissional_encontrado,
        pu.unidade_encontrada,

        pu.dt_atualiza

    from {{ ref("int_subpav_cnes_aps__profissionais_unidades") }} pu
    left join competencias_legado cl
        on pu.data_particao = cl.data_particao
),

profissionais as (
    select
        p.data_particao,
        p.data_particao as data_particao_origem_cnes,

        p.profissional_id_original,

        p.cpf,
        p.cns,
        p.nome_profissional,
        p.nome_social,

        p.dt_nascimento,
        p.sexo_id,
        p.sexo,
        p.raca_cor_id,
        p.nivel_escolaridade_id,

        p.nacionalidade,
        p.nacionalidade_indicador_original,
        p.nacionalidade_id_original,
        p.nome_pais_origem,

        p.telefone,
        p.email,

        p.cpf_valido,
        p.cns_preenchido

    from {{ ref("int_subpav_cnes_aps__profissionais") }} p
),

equipes_profissionais_snapshot as (
    select
        ep.data_particao,
        ep.data_particao as data_particao_origem_vinculo,
        'SNAPSHOT_MES' as origem_competencia_vinculo,
        0 as prioridade_origem_competencia_vinculo,
        ep.* except(data_particao)

    from {{ ref("int_subpav_cnes_aps__equipes_profissionais") }} ep
    where ep.equipe_ativa = 1
),

-- Compatibilização com o importador legado:
-- mantém o snapshot mensal como base, mas antecipa para a competência corrente
-- entradas que só aparecem na partição seguinte, desde que a dt_entrada esteja
-- dentro da janela da competência: data_particao <= dt_entrada <= dt_final_competencia.
-- Isso cobre casos como transferência de equipe registrada no fechamento da competência,
-- sem deslocar o snapshot inteiro para o mês anterior.
equipes_profissionais_entradas_fechamento as (
    select
        cl.data_particao,
        ep.data_particao as data_particao_origem_vinculo,
        'ENTRADA_FECHAMENTO_COMPETENCIA' as origem_competencia_vinculo,
        1 as prioridade_origem_competencia_vinculo,
        ep.* except(data_particao)

    from {{ ref("int_subpav_cnes_aps__equipes_profissionais") }} ep
    inner join competencias_legado cl
        on ep.data_particao = date_add(cl.data_particao, interval 1 month)
        and ep.dt_entrada between cl.data_particao and cl.dt_final_competencia

    where ep.equipe_ativa = 1
        and ep.vinculo_equipe_ativo = 1
        and ep.dt_desligamento is null
),

equipes_profissionais_competencia as (
    select *
    from equipes_profissionais_snapshot

    union all

    select *
    from equipes_profissionais_entradas_fechamento
),

equipes_profissionais_base as (
    select
        data_particao,

        data_particao_origem_vinculo,
        origem_competencia_vinculo,
        prioridade_origem_competencia_vinculo,

        cpf,
        cnes,
        cod_cbo,

        ine,
        tipo_equipe_id,
        tipo_equipe_descricao,
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

        dt_entrada,
        dt_desligamento,
        fl_equipeminima,
        possui_dt_desligamento,
        vinculo_equipe_ativo,
        microarea,
        tipo_enriquecimento_vinculo_unidade,

        count(*) over (
            partition by data_particao, cpf, cnes, cod_cbo
        ) as qtd_vinculos_equipe_mesma_chave,

        countif(vinculo_equipe_ativo = 1) over (
            partition by data_particao, cpf, cnes, cod_cbo
        ) as qtd_vinculos_ativos_mesma_chave

    from equipes_profissionais_competencia
),

equipes_profissionais_dedup as (
    select *
    from equipes_profissionais_base

    qualify row_number() over (
        partition by
            data_particao,
            cpf,
            cnes,
            cod_cbo,
            coalesce(ine, ''),
            coalesce(cast(tipo_equipe_id as string), '')
        order by
            prioridade_origem_competencia_vinculo desc,
            vinculo_equipe_ativo desc,
            fl_equipeminima desc,
            dt_entrada desc,
            dt_desligamento desc,
            ine asc
    ) = 1
),

consolidado as (
    select
        pu.data_particao,
        pu.ano_particao,
        pu.mes_particao,
        format_date('%Y%m', pu.data_particao) as competencia_mes,

        -- auditoria da janela da competência legada
        pu.dt_final_competencia,
        pu.dt_final_competencia_anterior,

        -- chaves originais
        pu.profissional_id_original,
        pu.unidade_id_original,

        -- identificação profissional
        pu.cpf,
        coalesce(p.cns, pu.cns) as cns,
        coalesce(p.nome_profissional, pu.nome_profissional) as nome_profissional,
        p.nome_social,

        p.dt_nascimento,
        p.sexo_id,
        p.sexo,
        p.raca_cor_id,
        p.nivel_escolaridade_id,

        p.nacionalidade,
        p.nacionalidade_indicador_original,
        p.nacionalidade_id_original,
        p.nome_pais_origem,

        p.telefone,
        p.email,

        p.cpf_valido,
        p.cns_preenchido,

        -- unidade
        pu.cnes,
        pu.ap,
        pu.ap_formatada,
        pu.nome_unidade,
        pu.is_municipio_rio,
        pu.tipo_unidade_sms,
        pu.is_unidade_aps_panorama,
        pu.unidade_ativa,

        -- vínculo unidade/CBO
        pu.cod_cbo,
        pu.vinculacao_id_original,
        pu.tipo_sus_nao_sus,
        pu.detalhe_terceirizado_sih,
        pu.cnpj_detalhe_vinculo,

        pu.conselho_id_original,
        pu.numero_registro,
        pu.uf_registro,

        pu.cg_horaamb,
        pu.cg_horahosp,
        pu.cg_horaoutr,
        pu.carga_horaria_total,
        pu.carga_horaria_classificacao,
        pu.possui_carga_horaria,

        pu.tp_preceptor,
        pu.tp_residente,

        -- vínculo com equipe
        ep.ine,
        ep.tipo_equipe_id,
        ep.tipo_equipe_descricao,
        ep.classificacao_equipe,
        ep.classificacao_equipe_temporal,
        ep.classificacao_equipe_historica,
        ep.classificacao_equipe_painel,

        ep.equipe_ativa,

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
        ep.fl_equipeminima,
        ep.possui_dt_desligamento,
        ep.vinculo_equipe_ativo,
        ep.microarea,
        ep.tipo_enriquecimento_vinculo_unidade,

        case
            when ep.dt_desligamento is not null
                then date_diff(
                    coalesce(pu.dt_final_competencia, last_day(pu.data_particao, month)),
                    ep.dt_desligamento,
                    day
                )
            else null
        end as dias_desligado,

        case
            when ep.qtd_vinculos_ativos_mesma_chave > 1 then 1
            else 0
        end as inconsistente,

        ep.qtd_vinculos_equipe_mesma_chave,
        ep.qtd_vinculos_ativos_mesma_chave,

        case
            when ep.ine is not null then 1
            else 0
        end as possui_vinculo_equipe,

        pu.profissional_encontrado,
        pu.unidade_encontrada,

        concat(
            coalesce(pu.cpf, ''),
            coalesce(pu.cnes, ''),
            coalesce(pu.cod_cbo, '')
        ) as chave_profissional_unidade_cbo,

        concat(
            coalesce(pu.cpf, ''),
            coalesce(pu.cnes, ''),
            coalesce(pu.cod_cbo, ''),
            coalesce(ep.ine, '')
        ) as chave_profissional_unidade_cbo_equipe,

        ep.data_particao_origem_vinculo,
        ep.origem_competencia_vinculo,

        pu.dt_atualiza as dt_atualiza_vinculo_unidade,
        pu.loaded_at,
        pu._source_file

    from profissionais_unidades pu

    left join profissionais p
        on pu.data_particao = p.data_particao
        and pu.data_particao_origem_cnes = p.data_particao_origem_cnes
        and pu.profissional_id_original = p.profissional_id_original

    left join equipes_profissionais_dedup ep
        on pu.data_particao = ep.data_particao
        and pu.cpf = ep.cpf
        and pu.cnes = ep.cnes
        and pu.cod_cbo = ep.cod_cbo
),

deduplicado as (
    select *
    from consolidado

    qualify row_number() over (
        partition by
            data_particao,
            cpf,
            cnes,
            cod_cbo,
            coalesce(ine, ''),
            coalesce(cast(tipo_equipe_id as string), '')
        order by
            possui_vinculo_equipe desc,
            vinculo_equipe_ativo desc,
            case
                when origem_competencia_vinculo = 'ENTRADA_FECHAMENTO_COMPETENCIA' then 1
                else 0
            end desc,
            fl_equipeminima desc,
            dt_entrada desc,
            dt_desligamento desc,
            loaded_at desc,
            dt_atualiza_vinculo_unidade desc
    ) = 1
)

select *
from deduplicado
