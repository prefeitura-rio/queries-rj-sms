{{ config(materialized="ephemeral") }}

with
    -- conjunto de pacientes para os quais queremos encontrar o cns
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
            safe_cast(paciente_cns as int64) as cns_fonte,
            {{ clean_name_string("paciente_nome") }} as nome_fonte,
            {{ clean_name_string("paciente_nome_mae") }} as nome_mae_fonte,
            date(paciente_dt_nasc) as data_nasc_fonte
        from {{ ref("raw_sisreg_api__marcacoes") }}
        where
            1 = 1
            and paciente_cns is not null
            and paciente_nome is not null
            and paciente_dt_nasc is not null
            and paciente_nome_mae is not null

        union all

        -- pacientes do hci
        select
            "hci" as fonte,
            safe_cast(cns_item as int64) as cns_fonte,
            {{ clean_name_string("upper(dados.nome)") }} as nome_fonte,
            {{ clean_name_string("upper(dados.mae_nome)") }} as nome_mae_fonte,
            dados.data_nascimento as data_nasc_fonte
        from {{ ref("mart_historico_clinico__paciente") }}
        cross join unnest(cns) as cns_item
        where
            1 = 1
            and cns_item is not null
            and dados.nome is not null
            and dados.mae_nome is not null
            and dados.data_nascimento is not null

        union all

        -- pacientes do sih
        select
            "sih" as fonte,
            safe_cast(paciente_cns as int64) as cns_fonte,
            {{ clean_name_string("upper(paciente_nome)") }} as nome_fonte,
            {{ clean_name_string("upper(paciente_mae_nome)") }} as nome_mae_fonte,
            date(paciente_data_nascimento) as data_nasc_fonte
        from {{ ref("raw_sih__autorizacoes_internacoes_hospitalares") }}
        where
            1 = 1
            and paciente_cns is not null
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
            fonte.cns_fonte,
            fonte.fonte,
            0.0 as score_lev,
            0.0 as score_jac,
            0.0 as score_final,
            sim.nome_sim  as nome_fonte,
            sim.nome_mae_sim as nome_mae_fonte
        from pacientes_sim as sim
        join
            fontes_externas as fonte
            on sim.nome_sim = fonte.nome_fonte
            and sim.nome_mae_sim = fonte.nome_mae_fonte
            and sim.data_nasc_sim = fonte.data_nasc_fonte
        where fonte.cns_fonte is not null
    ),

    candidatos_fuzzy as (
        select distinct
            sim.*,
            fonte.cns_fonte,
            fonte.nome_fonte,
            fonte.nome_mae_fonte,
            fonte.fonte 
        from pacientes_sim as sim
        join fontes_externas as fonte
          on sim.data_nasc_sim = fonte.data_nasc_fonte
        where
            not (
                sim.nome_sim = fonte.nome_fonte
                and sim.nome_mae_sim = fonte.nome_mae_fonte
            )  -- evitando recalcular para matches exatos
            -- CHANGED: optional guard to drop empty strings
            and fonte.cns_fonte is not null
    ),

    scores_fuzzy as (
        select
            nome_sim,
            nome_mae_sim,
            data_nasc_sim,
            declaracao_obito_sim,
            cns_fonte,
            nome_fonte,
            nome_mae_fonte,
            fonte,

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
            cns_fonte,
            nome_sim as nome_fonte,
            nome_mae_sim as nome_mae_fonte,
            fonte,
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
            cns_fonte,
            nome_fonte,
            nome_mae_fonte,
            fonte,
            score_lev,
            score_jac,
            score_final
        from scores_fuzzy_resumidos
    ),

    candidatos_filtrados as (
        select *
        from todos_scores
        where
            1 = 1
            and (score_final <= 0.4)
            and ((score_lev <= 0.2) or (score_jac <= 0.2))
    ),

    melhor_score as (
        select
            nome_sim,
            nome_mae_sim,
            data_nasc_sim,
            min(score_final) as min_score_final
        from candidatos_filtrados
        group by 1,2,3
    ),

    top_ties as (
        select f.*
        from candidatos_filtrados f
        join melhor_score b
          using (nome_sim, nome_mae_sim, data_nasc_sim)
        where abs(f.score_final - b.min_score_final) <= 1e-9
    ),

    top_ties_ranked as (
        select
            t.*,
            row_number() over (
                partition by nome_sim, nome_mae_sim, data_nasc_sim, declaracao_obito_sim, cns_fonte
                order by score_final asc, score_lev asc, score_jac asc, fonte asc, nome_fonte asc, nome_mae_fonte asc
            ) as rn_cns
        from top_ties t
    ),

    top_ties_cns as (
        select *
        from top_ties_ranked
        where rn_cns = 1
    ),

    agg as (
        select
            declaracao_obito_sim,
            data_nasc_sim,
            nome_sim,
            nome_mae_sim,

            array_agg(cns_fonte
                      order by score_final asc, score_lev asc, score_jac asc, cns_fonte asc) as cns_array,

            array_agg(
                struct(
                    cns_fonte as cns,
                    nome_fonte as nome_candidato,
                    nome_mae_fonte as nome_mae_candidato,
                    score_lev, score_jac, score_final
                )
                order by score_final asc, score_lev asc, score_jac asc, cns_fonte asc
            ) as candidatos_array

        from top_ties_cns
        group by 1,2,3,4
    )

select
    declaracao_obito_sim,
    data_nasc_sim as data_nasc,
    nome_sim as nome,
    (candidatos_array[OFFSET(0)]).nome_candidato as nome_candidato,
    nome_mae_sim as nome_mae,
    (candidatos_array[OFFSET(0)]).nome_mae_candidato as nome_mae_candidato,
    (candidatos_array[OFFSET(0)]).score_lev as score_lev,
    (candidatos_array[OFFSET(0)]).score_jac as score_jac,
    (candidatos_array[OFFSET(0)]).score_final as score_final,
    generate_uuid() as cns_id,
    cns_array,
    candidatos_array

from agg
