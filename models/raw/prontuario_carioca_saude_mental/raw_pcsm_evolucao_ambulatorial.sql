{{
    config(
        schema="brutos_prontuario_carioca_saude_mental",
        alias="evolucao_ambulatorial",
        materialized="table",
        tags=["raw", "pcsm", "material"],
        )
}}

with 

    source as (
        select * except(
            _airbyte_raw_id,
            _airbyte_extracted_at,
            _airbyte_meta,
            _airbyte_generation_id
            ) 
            from {{source('brutos_prontuario_carioca_saude_mental_staging', 'gh_evoluamb')}}
    ),

    renomeado as (
        select 
        seqatend as id_atendimento,
        seqpac as id_paciente,
        seqprof as id_profissional,
        sequs as id_unidade_saude,
        dtevopac as data_evolucao,
        dscevopac as descricao_evolucao,
        codcboprof as profissional_cbo,
        codcateg as codigo_categoria,

        -- COLUNAS QUE PRECISAM DE DOCUMENTAÇÃO
        dtcancpac,
        codabapac as codigo_abapac,
        indevodin,
        indevofav,
        dsclistpess,
        indtpevopac
        from source
    )

select * from renomeado