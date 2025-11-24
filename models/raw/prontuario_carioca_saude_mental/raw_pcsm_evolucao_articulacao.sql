{{
    config(
        schema="brutos_prontuario_carioca_saude_mental",
        alias="evolucao_articulacao",
        materialized="table",
        tags=["raw", "pcsm"],
        )
}}

with
    source as (
        select
            seqarticula as id_articulacao,
            seqevoarticula as id_evolucao_articulacao,
            seqpautaarticula as id_pauta_articulacao,
            seqprof as id_profissional,
            codcboprof as profissional_cbo,
            dtevoarticula as data_evolucao_articulacao,
            dscevoarticula as descricao_evolucao_articulacao,
            codabaarticula as codigo_aba_articulacao,

            -- NECESSITAM ARTICULACAO
            dsclistpac,
            dtcancarticula,
            indtpevoarticula,

        from {{source('brutos_prontuario_carioca_saude_mental_staging', 'gh_evoluarticula')}}
    )

select * from source