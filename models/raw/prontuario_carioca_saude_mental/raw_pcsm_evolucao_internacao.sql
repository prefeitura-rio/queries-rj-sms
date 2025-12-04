{{
    config(
        schema="brutos_prontuario_carioca_saude_mental",
        alias="evolucao_internacao",
        materialized="table",
        tags=["raw", "pcsm"],
        )
}}

with
    source as (
        select 
            seqinter as id_internacao,
            seqatend as id_atendimento,
            seqpac as id_paciente,
            sequs as id_unidade_saude,
            seqevopac as id_evolucao_internacao,
            dtevopac as data_evolucao_internacao,
            dscevopac as descricao_evolucao_internacao,
            seqprof as id_profissional,
            codcboprof as profissional_cbo,
            codcateg as codigo_categoria,

            -- NECESSITAM DE DOCUMENTAÇÃO
            codabapac as codigo_abapac,
            dtcancpac,
            indevofav,
            dsclistpess,
            indtpevopac

        from {{source('brutos_prontuario_carioca_saude_mental_staging', 'gh_evoluinter')}}
    )

select * from source 