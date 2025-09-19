{{
    config(
        alias="medilab_exames",
        schema="intermediario_historico_clinico",
        materialized="table",
        partition_by={
            "field": "exame_data_particao",
            "data_type": "date",
            "granularity": "day",
        }
    )
}}

with 
    estabelecimentos as (
        select
            id_cnes,
            nome_limpo
        from {{ ref('dim_estabelecimento') }}
    ),

    exames as (
        select
            *
        from {{ ref('raw_medilab__exames') }}
    ),
    agregado as (
        select
            ex.id_cnes,
            nome_limpo as unidade_nome,

            paciente_cpf,
            paciente_cns,
            {{ proper_br('paciente_nome') }} as paciente_nome,
            {{ proper_br('paciente_mae_nome') }} as paciente_mae_nome,
            paciente_data_nascimento,

            id_exame,
            id_laudo,
            exame_nome,
            exame_codigo_sigtap,
            exame_data,
            laudo_bucket,
            laudo_data_atualizacao,

            {{ proper_br('medico_requisitante') }} as medico_requisitante,
            {{ proper_br('medico_responsavel') }} as medico_responsavel,
            {{ proper_br('medico_revisor') }} as medico_revisor,

            exame_data_particao
        from exames ex
        left join estabelecimentos es on ex.id_cnes = es.id_cnes
    ),

    exames_com_cpf_validos as (
        select
            *
        from agregado
        where {{ validate_cpf("paciente_cpf") }}
    )

select * from exames_com_cpf_validos
