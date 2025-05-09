{{
    config(
        materialized="table",
        schema="projeto_c34",
        alias="cpfs_interesse",
        partition_by={
            "field": "data_nasc_sim",
            "data_type": "date",
            "granularity": "month",
        },
    )
}}

with
    -- conjunto de pacientes para os quais queremos encontrar o cpf
    pacientes_sim as (
        select distinct
            {{ clean_name_string("upper(nome)") }} as nome_sim,
            {{ clean_name_string("upper(nome_mae)") }} as nome_mae_sim,
            data_nasc as data_nasc_sim

        from {{ source("miloskimatheus__monitora_reg", "projeto_c34__sim_2024_mrj") }}

        where
            1 = 1
            and nome is not null
            and nome_mae is not null
            and data_nasc is not null
    ),

    fontes_externas as (

        -- pacientes do sisreg
        select
            "sisreg" as fonte,
            safe_cast(paciente_cpf as int64) as cpf_fonte,
            {{ clean_name_string("paciente_nome") }} as nome_fonte,
            {{ clean_name_string("paciente_nome_mae") }} as nome_mae_fonte,
            date(paciente_dt_nasc) as data_nasc_fonte
        from {{ ref("raw_sisreg_api__marcacoes") }}
        where
            1 = 1
            and paciente_cpf is not null
            and paciente_nome is not null
            and paciente_dt_nasc is not null
            and paciente_nome_mae is not null

        union all

        -- pacientes do hci
        select
            "hci" as fonte,
            safe_cast(cpf as int64) as cpf_fonte,
            {{ clean_name_string("upper(dados.nome)") }} as nome_fonte,
            {{ clean_name_string("upper(dados.mae_nome)") }} as nome_mae_fonte,
            dados.data_nascimento as data_nasc_fonte
        from {{ ref("mart_historico_clinico__paciente") }}
        where
            1 = 1
            and cpf is not null
            and dados.nome is not null
            and dados.mae_nome is not null
            and dados.data_nascimento is not null
    ),

    matches_exatos as (
        select distinct
            sim.nome_sim,
            sim.nome_mae_sim,
            sim.data_nasc_sim,
            fonte.cpf_fonte,
            0.0 as score_final
        from pacientes_sim as sim
        join
            fontes_externas as fonte
            on sim.nome_sim = fonte.nome_fonte
            and sim.nome_mae_sim = fonte.nome_mae_fonte
            and sim.data_nasc_sim = fonte.data_nasc_fonte
    ),

    candidatos_fuzzy as (
        select distinct sim.*, fonte.cpf_fonte, fonte.nome_fonte, fonte.nome_mae_fonte

        from pacientes_sim as sim
        join fontes_externas as fonte on sim.data_nasc_sim = fonte.data_nasc_fonte
        where
            not (
                sim.nome_sim = fonte.nome_fonte
                and sim.nome_mae_sim = fonte.nome_mae_fonte
            )  -- evitando recalcular para matches exatos
    ),

    scores_fuzzy as (
        select
            nome_sim,
            nome_mae_sim,
            data_nasc_sim,
            cpf_fonte,
            nome_fonte,
            nome_mae_fonte,

            {{ calculate_lev("nome_sim", "nome_fonte") }} as d_lev_nome,
            {{ calculate_lev("nome_mae_sim", "nome_mae_fonte") }} as d_lev_mae,
            {{ calculate_jaccard("nome_sim", "nome_fonte") }} as d_jac_nome,
            {{ calculate_jaccard("nome_mae_sim", "nome_mae_fonte") }} as d_jac_mae

        from candidatos_fuzzy
    ),

    scores_fuzzy_ponderados as (
        select
            *,

            (0.5 * d_lev_nome) + (0.5 * d_lev_mae) as s_lev,
            (0.5 * d_jac_nome) + (0.5 * d_jac_mae) as s_jac,

            (
                (0.25 * d_lev_nome)
                + (0.25 * d_lev_mae)
                + (0.25 * d_jac_nome)
                + (0.25 * d_jac_mae)
            ) as score_final

        from scores_fuzzy
        where
            (d_lev_nome <= 0.3 and d_lev_mae <= 0.3)
            or (d_jac_nome <= 0.3 and d_jac_mae <= 0.3)
    ),

    todos_scores as (
        select
            nome_sim,
            nome_mae_sim,
            data_nasc_sim,
            cpf_fonte,
            nome_sim as nome_fonte,
            nome_mae_sim as nome_mae_fonte,
            score_final
        from matches_exatos

        union all

        select
            nome_sim,
            nome_mae_sim,
            data_nasc_sim,
            cpf_fonte,
            nome_fonte,
            nome_mae_fonte,
            score_final
        from scores_fuzzy_ponderados
    ),

    ranking_scores as (
        select
            *,
            row_number() over (
                partition by nome_sim, nome_mae_sim, data_nasc_sim
                order by score_final asc
            ) as rn
        from todos_scores
    )

select
    data_nasc_sim as data_nasc,
    nome_sim as nome,
    nome_fonte as nome_candidato,
    nome_mae_sim as nome_mae,
    nome_mae_fonte as nome_mae_candidato,
    cpf_fonte as cpf_candidato,
    score_final as score_incerteza
from ranking_scores
order by score_final desc
