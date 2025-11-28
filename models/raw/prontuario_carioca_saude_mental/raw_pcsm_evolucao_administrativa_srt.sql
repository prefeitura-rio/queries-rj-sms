{{
    config(
        schema="brutos_prontuario_carioca_saude_mental",
        alias="evolucao_administrativa_srt",
        materialized="table",
        tags=["raw", "pcsm", "material"],
        )
}}

with

    source as (
        select 
            seqsrt as id_srt,
            seqlogin as id_login,
            seqevoadmsrt as id_evolucao_srt,
            dtevoadmsrt as data_evolucao_srt,
            dscevoadmsrt as descricao_evolucao_srt,

            -- NECESSITAM DE DOCUMENTAÇÃO
            codabaadmsrt as codigo_abaadmsrt,
            indtpevogrp,
            dtcancadmsrt
        from {{source('brutos_prontuario_carioca_saude_mental_staging', 'gh_evoluadmsrt')}}
    )

select * from source