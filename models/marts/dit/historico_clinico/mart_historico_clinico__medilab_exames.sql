{{
    config(
        schema="saude_historico_clinico",
        alias="medilab_exames",
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
            paciente_cns as paciente_cns,
            paciente_nome as paciente_nome,
            paciente_mae_nome as paciente_mae_nome,
            paciente_data_nascimento as paciente_data_nascimento,
            exame_data as exame_data,
            exame_nome as exame_nome,
            exame_codigo_sigtap as exame_codigo_sigtap,
            safe_cast(paciente_cpf as int) as cpf_particao 
        from medilab_exames
    )

select * from renamed