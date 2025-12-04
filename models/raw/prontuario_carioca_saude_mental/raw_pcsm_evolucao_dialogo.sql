{{
    config(
        schema="brutos_prontuario_carioca_saude_mental",
        alias="evolucao_dialogo",
        materialized="table",
        tags=["raw", "pcsm"],
        )
}}

with 
    source as (
        select
            seqlogin as id_login,
            seqdialog as id_dialogo,
            seqevodialog as id_evolucao_dialogo,
            seqpautadialog as id_pauta_dialogo,
            dtevodialog as data_evolucao_dialogo,
            dscevodialog as descricao_evolucao_dialogo,
            codabadialog as codigo_aba_dialogo,

            -- NECESSITAM DOCUMENTAÇÃO
            dsclistpac,
            dtcancdialog,
            indtpevodialog
        from {{source('brutos_prontuario_carioca_saude_mental_staging', 'gh_evoludialog')}}
    )

select * from source