{{
    config(
        schema="brutos_prontuario_carioca_saude_mental",
        alias="evolucao_matriciamento",
        materialized="table",
        tags=["raw", "pcsm"],
        )
}}

with 
    source as (
        select 
            seqmatric as id_matriciamento,
            seqevoinst as id_evolucao_matriciamento,
            seqpautamatric as id_pauta_matriciamento,
            dtevoinst as data_evolucao_matriciamento,
            dscevoinst as descricao_evolucao_matriciamento,
            codabainst as codigo_aba_matriciamento,
            seqprof as id_profissional,
            codcboprof as profissional_cbo,

            -- NECESSITAM DE DOCUMENTAÇÃO  
            dsclistpac,
            dtcancinst,
            indtpevogrp

        from {{source('brutos_prontuario_carioca_saude_mental_staging', 'gh_evoluinstit')}}
    )

select * from source