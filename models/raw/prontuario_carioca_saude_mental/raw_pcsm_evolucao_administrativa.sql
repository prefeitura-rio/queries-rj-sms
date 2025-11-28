{{
    config(
        schema="brutos_prontuario_carioca_saude_mental",
        alias="evolucao_administrativa",
        materialized="table",
        tags=["raw", "pcsm", "material"],
        )
}}

with 

    source as (
        select * 
        except(
            _airbyte_raw_id,
            _airbyte_extracted_at,
            _airbyte_meta,
            _airbyte_generation_id
            ) 
            from {{source('brutos_prontuario_carioca_saude_mental_staging', 'gh_evoluadm')}}
        ),


    renomeado as (
        select 
            seqevopac as id_evolucao,
            seqpac as id_paciente,
            sequs as id_unidade,
            seqlogin as id_login,
            dtevopac as data_evolucao,
            dscevopac as descricao_evolucao,
            
            -- NECESSITAM DE DOCUMENTAÇÃO
            codcateg as codigo_categoria,
            codabapac as codigo_abapac,
            dtcancpac,
            indtpevopac
        from source
    )

select * from renomeado