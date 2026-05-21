{{
    config(
        schema = 'intermediario_plataforma_subpav',
        alias = 'cnes_aps__profissionais_consolidacao',
        materialized = "table",
        tags = ["subpav", "cnes_aps"]
    )
}}

with profissionais_unidades as (
    select
        data_particao,
        ano_particao,
        mes_particao,
        loaded_at,
        _source_file,

        profissional_id_original,
        unidade_id_original,
        cpf,
        cns,
        nome_profissional,
        cnes,
        ap,
        ap_formatada,
        nome_unidade,

        cod_cbo,
        cbo_id_original,
        vinculacao_id_original,
        conselho_id_original,
        numero_registro,
        uf_registro,

        cg_horaamb,
        cg_horahosp,
        cg_horaoutr,
        carga_horaria_total,
        carga_horaria_classificacao,

        tp_preceptor,
        tp_residente,
        dt_inicio_atividade,
        dt_atualiza
    from {{ ref("int_subpav_cnes_aps__profissionais_unidades") }}
),

profissionais as (
    select
        data_particao,
        profissional_id_original,
        cpf,
        cns,
        nome,
        dt_nasc,
        sexo_id,
        raca_cor_id,
        nivel_escolaridade_id,
        ind_nacio,
        nome_pais,
        telefone,
        email
    from {{ ref("int_subpav_cnes_aps__profissionais") }}
),

equipes_profissionais_base as (
    select
        data_particao,
        cpf,
        cnes,
        cod_cbo,
        ine,
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
    from {{ ref("int_subpav_cnes_aps__equipes_profissionais") }}
    where equipe_ativa = 1
),

equipes_profissionais_dedup as (
    select *
    from equipes_profissionais_base
    qualify row_number() over (
        partition by data_particao, cpf, cnes, cod_cbo
        order by
            vinculo_equipe_ativo desc,
            fl_equipeminima desc,
            dt_entrada asc,
            dt_desligamento desc
    ) = 1
),

consolidado as (
    select
        pu.data_particao,
        pu.ano_particao,
        pu.mes_particao,
        format_date('%Y%m', pu.data_particao) as competencia_mes,

        -- chaves originais
        pu.profissional_id_original,
        pu.unidade_id_original,

        -- identificação profissional
        pu.cpf,
        coalesce(p.cns, pu.cns) as cns,
        coalesce(p.nome, pu.nome_profissional) as nome_profissional,
        p.dt_nasc,
        p.sexo_id,
        p.raca_cor_id,
        p.nivel_escolaridade_id,
        p.ind_nacio,
        p.nome_pais,
        p.telefone,
        p.email,

        -- unidade
        pu.cnes,
        pu.ap,
        pu.ap_formatada,
        pu.nome_unidade,

        -- vínculo unidade/CBO
        pu.cod_cbo,
        pu.cbo_id_original,
        pu.vinculacao_id_original,
        pu.conselho_id_original,
        pu.numero_registro,
        pu.uf_registro,

        pu.cg_horaamb,
        pu.cg_horahosp,
        pu.cg_horaoutr,
        pu.carga_horaria_total,
        pu.carga_horaria_classificacao,

        pu.tp_preceptor,
        pu.tp_residente,

        -- vínculo com equipe
        ep.ine,
        ep.tipo_equipe_id,
        ep.classificacao_equipe,
        ep.classificacao_equipe_historica,
        ep.classificacao_equipe_painel,
        ep.equipe_ativa,
        ep.is_equipe_aps_cobertura,
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

        case
            when ep.dt_desligamento is not null
                then date_diff(last_day(pu.data_particao, month), ep.dt_desligamento, day)
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

        pu.dt_inicio_atividade,
        pu.dt_atualiza as dt_atualiza_vinculo_unidade,
        pu.loaded_at,
        pu._source_file

    from profissionais_unidades pu

    left join profissionais p
        on pu.data_particao = p.data_particao
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
        partition by data_particao, cpf, cnes, cod_cbo, coalesce(ine, '')
        order by
            possui_vinculo_equipe desc,
            vinculo_equipe_ativo desc,
            fl_equipeminima desc,
            dt_entrada asc,
            loaded_at desc,
            dt_atualiza_vinculo_unidade desc
    ) = 1
)

select *
from deduplicado
