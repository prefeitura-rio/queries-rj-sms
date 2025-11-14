{{
    config(
        schema="brutos_prontuario_carioca_saude_mental",
        alias="evolucao_apoio",
        materialized="table",
        tags=["raw", "pcsm", "material"],
        )
}}

with 
    source as (
        select 
            seqapoio as id_apoio,
            seqevoapoio as id_evolucao_apoio,
            seqpautaapoio as id_pauta_apoio,
            dtevoapoio as data_evolucao_apoio,
            dscevoapoio as descricao_apoio,
            seqprof as id_profissional,
            codcboprof as profissional_cbo,
            codabaapoio as codigo_aba_apoio,

            -- NECESSITAM DOCUMENTAÇÃO
            dtcancapoio,
            indtpevoapoio
        from {{source('brutos_prontuario_carioca_saude_mental_staging', 'gh_evoluapoio')}}
    )

select * from source
    