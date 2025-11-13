{{
    config(
        schema="brutos_prontuario_carioca_saude_mental",
        alias="evolucao_paciente",
        materialized="table",
        tags=["raw", "pcsm"],
        )
}}

with 
    source as (
        select
            seqatend as id_atendimento,
            seqpac as id_paciente,
            sequs as id_unidade_saude,
            seqevopac as id_evolucao_paciente,
            dtevopac as data_evolucao_paciente,
            dscevopac as descricao_evolucao_paciente,
            seqprof as id_profissional,
            codcboprof as profissional_cbo,
            codcateg as codigo_categoria,

            -- NECESSITAM DOCUMENTAÇÃO
            codabapac as codigo_abapac,
            dtcancpac,
            indevodin,
            indevofav,
            dsclistpess,
            indtpevopac
            
        from {{source('brutos_prontuario_carioca_saude_mental_staging', 'gh_evolupac')}}
    )

    select * from source