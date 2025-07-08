{{ 
    config(
        materialized="table",
        alias="sim"
    )
}}

with
    pacientes_sim as (
        select distinct
            {{ clean_name_string("upper(nome)") }} as nome_sim,
            {{ clean_name_string("upper(nome_mae)") }} as nome_mae_sim,
            data_nascimento as data_nascimento_sim,
            id_hash as id_hash_declaracao_obito
        from {{ ref("raw_sim__mortalidades") }}

        where
            nome is not null
            and nome_mae is not null
            and data_nascimento is not null
    ),
    pacientes_hci as (
        select
            safe_cast(cpf as int64) as cpf_fonte,
            {{ clean_name_string("upper(dados.nome)") }} as nome_fonte,
            {{ clean_name_string("upper(dados.mae_nome)") }} as nome_mae_fonte,
            dados.data_nascimento as data_nascimento_fonte
        from {{ ref("mart_historico_clinico__paciente") }}
        where
            dados.nome is not null
            and dados.mae_nome is not null
            and dados.data_nascimento is not null
    ),
    scores_fuzzy as (
        select
            nome_sim,
            nome_mae_sim,
            data_nascimento_sim,
            id_hash_declaracao_obito,
            cpf_fonte,
            nome_fonte,
            nome_mae_fonte,

            {{ calculate_lev("nome_sim", "nome_fonte") }} as d_lev_nome,
            {{ calculate_lev("nome_mae_sim", "nome_mae_fonte") }} as d_lev_mae,
            {{ calculate_jaccard("nome_sim", "nome_fonte") }} as d_jac_nome,
            {{ calculate_jaccard("nome_mae_sim", "nome_mae_fonte") }} as d_jac_mae

        from pacientes_sim as sim
        join pacientes_hci as fonte on sim.data_nascimento_sim = fonte.data_nascimento_fonte
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
    ranking_scores as (
        select
            *,
            row_number() over (
                partition by nome_sim, nome_mae_sim, data_nascimento_sim
                order by score_final asc
            ) as rn
        from scores_fuzzy_resumidos
    )

select distinct
    id_hash_declaracao_obito,
    to_hex(sha256(cast(cpf_fonte as string))) as id_hash_paciente,
    data_nascimento_sim as data_nasc,
    nome_sim as nome,
    nome_fonte as nome_candidato,
    nome_mae_sim as nome_mae,
    nome_mae_fonte as nome_mae_candidato,
    cpf_fonte as cpf_candidato,
    score_lev,
    score_jac,
    score_final
from ranking_scores
where
    rn = 1
    and ((score_lev <= 0.3) or (score_jac <= 0.3))
    and score_final <= 0.4