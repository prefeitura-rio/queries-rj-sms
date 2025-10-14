{{
    config(
        alias="vacinacao",
        materialized="table",
    )
}}

with
    vacinacoes_historico as (
        select 
            * 
        from {{ ref('raw_prontuario_vitacare_historico__vacina') }}
    ),
    atendimentos_historico as (
        select 
            id_prontuario_global,
            ut_id
        from {{ ref('raw_prontuario_vitacare_historico__atendimento') }}
    ),
    pacientes_historico as (
        select 
            p.codigo_equipe,
            p.ine_equipe,
            p.microarea,
            p.npront,
            p.cns,
            p.cpf,
            p.equipe,
            p.nome,
            p.sexo,
            p.data_nascimento,
            p.nome_mae,
            p.situacao_usuario,
            p.data_cadastro,
            p.obito,
            ut_id
        from {{ ref('raw_prontuario_vitacare_historico__paciente') }}
    ),

    agg_renamed as (
        select 
            v.id_vacinacao,
            v.id_cnes,
            p.codigo_equipe as id_equipe,
            p.ine_equipe as id_equipe_ine,
            p.microarea as id_microarea,
            p.npront as paciente_id_prontuario,
            p.cns as paciente_cns,
            p.cpf as paciente_cpf,
            p.equipe as equipe_nome,
            a.profissional_nome,
            a.profissional_cbo,
            a.profissional_cns,
            a.profissional_cpf,
            v.nome_vacina as vacina_descricao,
            v.dose as vacina_dose,
            v.lote as vacina_lote,
            v.tipo_registro as vacina_registro_tipo,
            v.estrategia_imunizacao as vacina_estrategia,
            v.diff as vacina_diff,
            v.data_aplicacao as vacina_aplicacao_data,
            v.data_registro as vacina_registro_data,
            p.nome as paciente_nome,
            p.sexo as paciente_sexo,
            p.data_nascimento as paciente_nascimento_data,
            p.nome_mae as paciente_nome_mae,
            null as paciente_mae_nascimento_data,,
            p.situacao_usuario as paciente_situacao,
            p.data_cadastro as paciente_cadastro_data,
            p.obito as paciente_obito,
            v.id_cnes as requisicao_id_cnes,
            null as requisicao_area_programatica,
            null as requisicao_endpoint,
            null as metadados,
            null as particao_data_vacinacao
        from vacinacoes_historico v
        left join atendimentos_historico a 
            on v.id_prontuario_global = a.id_prontuario_global

    )


        pacientes as (
            select
                id_vacinacao,
                id_surrogate,
                id_cnes,
                id_equipe,
                id_equipe_ine,
                id_microarea,
                paciente_id_prontuario,
                paciente_cns,
                paciente_cpf,
                estabelecimento_nome,
                equipe_nome,
                profissional_nome,
                profissional_cbo,
                profissional_cns,
                profissional_cpf,
                vacina_descricao,
                vacina_dose,
                vacina_lote,
                vacina_registro_tipo,
                vacina_estrategia,
                vacina_diff,
                vacina_aplicacao_data,
                vacina_registro_data,
                paciente_nome,
                paciente_sexo,
                paciente_nascimento_data,
                paciente_nome_mae,
                paciente_mae_nascimento_data,
                paciente_situacao,
                paciente_cadastro_data,
                paciente_obito,
                requisicao_id_cnes,
                requisicao_area_programatica,
                requisicao_endpoint,
                metadados,
                particao_data_vacinacao
            from source
        )

    select * 
    from source