{{ config(materialized="table", schema="projeto_c34", alias="cpfs_fuzzy_match") }}

with
    -- conjunto de pacientes para os quais queremos encontrar o cpf
    pacientes_sim as (
        select distinct
            {{ clean_name_string("upper(nome)") }} as nome_sim,
            {{ clean_name_string("upper(nome_mae)") }} as nome_mae_sim,
            data_nasc as data_nasc_sim,
            declaracao_obito_num as declaracao_obito_sim

        from {{ source("sub_geral_prod", "c34_obitos_mrj") }}

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

        union all

        -- pacientes do sih
        select
            "sih" as fonte,
            safe_cast(paciente_cpf as int64) as cpf_fonte,
            {{ clean_name_string("upper(paciente_nome)") }} as nome_fonte,
            {{ clean_name_string("upper(paciente_mae_nome)") }} as nome_mae_fonte,
            date(paciente_data_nascimento) as data_nasc_fonte
        from {{ ref("raw_sih__autorizacoes_internacoes_hospitalares") }}
        where
            1 = 1
            and paciente_cpf is not null
            and paciente_nome is not null
            and paciente_data_nascimento is not null
            and paciente_mae_nome is not null
    ),

    matches_exatos as (
        select distinct
            sim.nome_sim,
            sim.nome_mae_sim,
            sim.data_nasc_sim,
            sim.declaracao_obito_sim,
            fonte.cpf_fonte,
            0.0 as score_lev,
            0.0 as score_jac,
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
            declaracao_obito_sim,
            cpf_fonte,
            nome_fonte,
            nome_mae_fonte,

            {{ calculate_lev("nome_sim", "nome_fonte") }} as d_lev_nome,
            {{ calculate_lev("nome_mae_sim", "nome_mae_fonte") }} as d_lev_mae,
            {{ calculate_jaccard("nome_sim", "nome_fonte") }} as d_jac_nome,
            {{ calculate_jaccard("nome_mae_sim", "nome_mae_fonte") }} as d_jac_mae

        from candidatos_fuzzy
    ),

    scores_fuzzy_resumidos as (
        select
            *,

            (0.5 * d_lev_nome) + (0.5 * d_lev_mae) as score_lev,
            (0.5 * d_jac_nome) + (0.5 * d_jac_mae) as score_jac,

            (0.25 * d_lev_nome)
            + (0.25 * d_lev_mae)
            + (0.25 * d_jac_nome)
            + (0.25 * d_jac_mae) as score_final

        from scores_fuzzy
    ),

    todos_scores as (
        select
            nome_sim,
            nome_mae_sim,
            data_nasc_sim,
            declaracao_obito_sim,
            cpf_fonte,
            nome_sim as nome_fonte,
            nome_mae_sim as nome_mae_fonte,
            score_lev,
            score_jac,
            score_final
        from matches_exatos

        union all

        select
            nome_sim,
            nome_mae_sim,
            data_nasc_sim,
            declaracao_obito_sim,
            cpf_fonte,
            nome_fonte,
            nome_mae_fonte,
            score_lev,
            score_jac,
            score_final
        from scores_fuzzy_resumidos
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
    declaracao_obito_sim,
    data_nasc_sim as data_nasc,
    nome_sim as nome,
    nome_fonte as nome_candidato,
    nome_mae_sim as nome_mae,
    nome_mae_fonte as nome_mae_candidato,
    cpf_fonte as cpf_candidato,
    to_hex(sha256(cast(cpf_fonte as string))) as id_paciente,
    score_lev,
    score_jac,
    score_final
from ranking_scores
where
    1 = 1
    and rn = 1
    and ((score_lev <= 0.2) or (score_jac <= 0.2))
    and score_final <= 0.4
