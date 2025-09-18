{{
    config(
        schema="saude_historico_clinico",
        alias="exame_imagem",
        materialized="table",
        partition_by={
            "field": "cpf_particao",
            "data_type": "int64",
            "range": {"start": 0, "end": 100000000000, "interval": 34722222},
        },
    )
}}

with 
    medilab_exames as (
        select 
            *
        from {{ ref('int_historico_clinico__exames__medilab') }}
    ),

    renamed as(
        select
            id_cnes as unidade_cnes,
            unidade_nome as unidade_nome,
            id_laudo as id_laudo,
            laudo_bucket as laudo_bucket,
            id_exame as id_exame,
            laudo_data_atualizacao as laudo_data_atualizacao,
            medico_requisitante as medico_requisitante,
            medico_responsavel as medico_responsavel,
            medico_revisor as medico_revisor,
            paciente_cpf as paciente_cpf,
            exame_data as exame_data,
            exame_nome as exame_nome,
            exame_codigo_sigtap as exame_codigo_sigtap,
            safe_cast(paciente_cpf as int) as cpf_particao 
        from medilab_exames
    )

select * from renamed