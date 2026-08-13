{{
  config(
    schema="projeto_teleconsulta",
    alias="teleconsultas",
  )
}}

with

    dedup_paciente as (
        select *
        from {{ ref("raw_prontuario_vitacare_historico__cadastro") }}
        qualify row_number() over (partition by id_global order by loaded_at desc) = 1
    ),

    dedup_profissional as (
        select *
        from {{ ref("raw_prontuario_vitacare_historico__profissional") }}
        qualify row_number() over (partition by id_global order by loaded_at desc) = 1
    ),

    condicoes_ativas as (
        select
            id_prontuario_global as id_atendimento,
            array_agg(cod_cid10) as condicoes_ativas
        from {{ ref("raw_prontuario_vitacare_historico__condicao") }}
        where estado = 'ATIVO'
        group by 1
    ),

    estabelecimentos as (
        select id_cnes, area_programatica, nome_limpo
        from {{ ref("dim_estabelecimento") }}
    ),

    agendamentos_tele as (
        select
            age.id_global as id_agendamento,
            cast(null as string) as id_atendimento,
            age.id_cnes,

            upper(cad.nome) as nome,
            cad.cpf,
            cad.cns,
            cad.data_nascimento,
            cad.sexo,
            cad.raca_cor,
            cad.bairro as bairro_residencia,
            cad.id_cnes as id_unidade_referencia,
            cad.equipe,
            cad.ine_equipe,

            {{ proper_br('prof.profissional_nome') }} as profissional_nome,
            prof.profissional_cbo as profissional_cbo,
            prof.profissional_cbo_descricao as profissional_cbo_descricao,
            prof.profissional_equipe_nome as profissional_equipe_nome,
            prof.profissional_equipe_cod_ine as profissional_equipe_cod_ine,

            datahora_agendamento as dthr_marcacao,
            safe_cast(null as datetime) as dthr_inicio_atendimento,
            safe_cast(null as datetime) as dthr_fim_atendimento,

            cast(null as string) as eh_coleta,
            tipo_consulta,

            estado_marcacao,
            motivo,

            cast(null as string) as subjetivo_motivo,
            cast(null as string) as plano_observacoes,
            cast(null as string) as avaliacao_observacoes,
            cast(null as string) as notas_observacoes,

            age.loaded_at
        from {{ ref("raw_prontuario_vitacare_historico__agendamento") }} age
            left join dedup_profissional prof on prof.id_global = age.id_profissional
            left join dedup_paciente cad on cad.id_global = age.id_paciente_global
        where
            tipo_atendimento = 'TELECONSULTA'
    ),
    agendamentos_tele_api as (
        select
            age.id_agendamento,
            cast(null as string) as id_atendimento,
            age.id_cnes,

            upper(coalesce(cad_primary.nome, cad_fallback.nome)) as nome,
            coalesce(cad_primary.cpf, cad_fallback.cpf) as cpf,
            coalesce(cad_primary.cns, cad_fallback.cns) as cns,
            coalesce(cad_primary.data_nascimento, cad_fallback.data_nascimento) as data_nascimento,
            coalesce(cad_primary.sexo, cad_fallback.sexo) as sexo,
            coalesce(cad_primary.raca_cor, cad_fallback.raca_cor) as raca_cor,
            coalesce(cad_primary.bairro, cad_fallback.bairro) as bairro_residencia,
            coalesce(cad_primary.id_cnes, cad_fallback.id_cnes) as id_unidade_referencia,
            coalesce(cad_primary.equipe, cad_fallback.equipe) as equipe,
            coalesce(cad_primary.ine_equipe, cad_fallback.ine_equipe) as ine_equipe,

            {{ proper_br('age.profissional_nome') }} as profissional_nome,
            age.profissional_cbo as profissional_cbo,
            age.profissional_cbo_descricao as profissional_cbo_descricao,
            age.profissional_equipe_nome as profissional_equipe_nome,
            age.profissional_equipe_ine as profissional_equipe_cod_ine,

            age.datahora_agendamento as dthr_marcacao,
            safe_cast(null as datetime) as dthr_inicio_atendimento,
            safe_cast(null as datetime) as dthr_fim_atendimento,

            cast(null as string) as eh_coleta,
            cast(null as string) as tipo_consulta,

            age.estado_marcacao,
            age.motivo_cancelamento as motivo,

            cast(null as string) as subjetivo_motivo,
            cast(null as string) as plano_observacoes,
            cast(null as string) as avaliacao_observacoes,
            cast(null as string) as notas_observacoes,

            age.source_updated_at as loaded_at
        from {{ ref("raw_prontuario_vitacare_api__agendamento") }} age
            left join dedup_paciente cad_primary
                on cad_primary.id_global = concat(age.id_cnes, '.', age.ut_id)
            left join dedup_paciente cad_fallback
                on cad_primary.id_global is null
                and cad_fallback.cpf = age.patient_cpf
        where
            age.tipo_atendimento = 'Agendamento Online'
    ),

    atendimentos_tele_api as (
        select
            cast(null as string) as id_agendamento,
            acto.id_prontuario_global as id_atendimento,
            acto.id_cnes,

            upper(coalesce(cad_primary.nome, cad_fallback.nome)) as nome,
            coalesce(cad_primary.cpf, cad_fallback.cpf) as cpf,
            coalesce(cad_primary.cns, cad_fallback.cns) as cns,
            coalesce(cad_primary.data_nascimento, cad_fallback.data_nascimento) as data_nascimento,
            coalesce(cad_primary.sexo, cad_fallback.sexo) as sexo,
            coalesce(cad_primary.raca_cor, cad_fallback.raca_cor) as raca_cor,
            coalesce(cad_primary.bairro, cad_fallback.bairro) as bairro_residencia,
            coalesce(cad_primary.id_cnes, cad_fallback.id_cnes) as id_unidade_referencia,
            coalesce(cad_primary.equipe, cad_fallback.equipe) as equipe,
            coalesce(cad_primary.ine_equipe, cad_fallback.ine_equipe) as ine_equipe,

            {{ proper_br('acto.profissional_nome') }} as profissional_nome,
            acto.profissional_cbo as profissional_cbo,
            acto.profissional_cbo_descricao as profissional_cbo_descricao,
            acto.profissional_equipe_nome as profissional_equipe_nome,
            acto.profissional_equipe_cod_ine as profissional_equipe_cod_ine,

            acto.datahora_marcacao_atendimento as dthr_marcacao,
            safe_cast(acto.datahora_inicio_atendimento as datetime) as dthr_inicio_atendimento,
            safe_cast(acto.datahora_fim_atendimento as datetime) as dthr_fim_atendimento,

            cast(acto.eh_coleta as string) as eh_coleta,
            acto.tipo_consulta,

            'EXECUTADO' as estado_marcacao,
            'N/A' as motivo,

            acto.subjetivo_motivo,
            acto.plano_observacoes,
            acto.avaliacao_observacoes,
            acto.notas_observacoes,

            acto.loaded_at
        from {{ ref("raw_prontuario_vitacare_api__acto") }} acto
            left join dedup_paciente cad_primary
                on cad_primary.id_global = concat(acto.id_cnes, '.', acto.ut_id)
            left join dedup_paciente cad_fallback
                on cad_primary.id_global is null
                and cad_fallback.cpf = acto.patient_cpf
        where
            acto.tipo_atendimento = 'Agendamento Online'
    ),

    atendimentos_tele as (
        select
            acto.id_global as id_atendimento,
            cast(null as string) as id_agendamento,
            acto.id_cnes,

            upper(cad.nome) as nome,
            cad.cpf,
            cad.cns,
            cad.data_nascimento,
            cad.sexo,
            cad.raca_cor,
            cad.bairro as bairro_residencia,
            cad.id_cnes as id_unidade_referencia,
            cad.equipe,
            cad.ine_equipe,

            {{ proper_br('profissional_nome') }} as profissional_nome,
            profissional_cbo as profissional_cbo,
            profissional_cbo_descricao as profissional_cbo_descricao,
            profissional_equipe_nome as profissional_equipe_nome,
            profissional_equipe_cod_ine as profissional_equipe_cod_ine,

            datahora_marcacao_atendimento as dthr_marcacao,
            safe_cast(datahora_inicio_atendimento as datetime) as dthr_inicio_atendimento,
            safe_cast(datahora_fim_atendimento as datetime) as dthr_fim_atendimento,

            cast(eh_coleta as string) as eh_coleta,
            tipo_consulta,

            'EXECUTADO' as estado_marcacao,
            'N/A' as motivo,

            subjetivo_motivo,
            plano_observacoes,
            avaliacao_observacoes,
            notas_observacoes,

            acto.loaded_at
        from
            {{ ref("raw_prontuario_vitacare_historico__acto") }} acto
            left join dedup_paciente cad on cad.id_global = acto.id_paciente_global
        where
            tipo_atendimento = 'TELECONSULTA'
    ),
    juncao as (
        select 
            id_agendamento, id_atendimento, id_cnes, nome, cpf, cns, data_nascimento, sexo, raca_cor, 
            bairro_residencia, equipe, ine_equipe, 
            profissional_nome, profissional_cbo, profissional_cbo_descricao, profissional_equipe_nome, profissional_equipe_cod_ine,
            tipo_consulta, dthr_marcacao, dthr_inicio_atendimento, dthr_fim_atendimento, 
            estado_marcacao, motivo, eh_coleta, subjetivo_motivo, plano_observacoes, avaliacao_observacoes, notas_observacoes
        from agendamentos_tele

        union all

        select 
            id_agendamento, id_atendimento, id_cnes, nome, cpf, cns, data_nascimento, sexo, raca_cor, 
            bairro_residencia, equipe, ine_equipe, 
            profissional_nome, profissional_cbo, profissional_cbo_descricao, profissional_equipe_nome, profissional_equipe_cod_ine,
            tipo_consulta, dthr_marcacao, dthr_inicio_atendimento, dthr_fim_atendimento, 
            estado_marcacao, motivo, eh_coleta, subjetivo_motivo, plano_observacoes, avaliacao_observacoes, notas_observacoes
        from atendimentos_tele

        union all

        select 
            id_agendamento, id_atendimento, id_cnes, nome, cpf, cns, data_nascimento, sexo, raca_cor, 
            bairro_residencia, equipe, ine_equipe, 
            profissional_nome, profissional_cbo, profissional_cbo_descricao, profissional_equipe_nome, profissional_equipe_cod_ine,
            tipo_consulta, dthr_marcacao, dthr_inicio_atendimento, dthr_fim_atendimento, 
            estado_marcacao, motivo, eh_coleta, subjetivo_motivo, plano_observacoes, avaliacao_observacoes, notas_observacoes
        from agendamentos_tele_api

        union all

        select 
            id_agendamento, id_atendimento, id_cnes, nome, cpf, cns, data_nascimento, sexo, raca_cor, 
            bairro_residencia, equipe, ine_equipe, 
            profissional_nome, profissional_cbo, profissional_cbo_descricao, profissional_equipe_nome, profissional_equipe_cod_ine,
            tipo_consulta, dthr_marcacao, dthr_inicio_atendimento, dthr_fim_atendimento, 
            estado_marcacao, motivo, eh_coleta, subjetivo_motivo, plano_observacoes, avaliacao_observacoes, notas_observacoes
        from atendimentos_tele_api
    ),

    normaliza_estado_marcacao as (
        select
            * replace (
                case upper(estado_marcacao)
                    when 'EXECUTADO'                  then 'Executado'
                    when 'AUSENTE'                    then 'Faltou'
                    when 'FALTOU'                     then 'Faltou'
                    when 'CANCELADO PELO PACIENTE'    then 'Cancelado pelo Paciente'
                    when 'CANCELADO PELO PROFISSIONAL' then 'Cancelado pelo Profissional'
                    else estado_marcacao
                end as estado_marcacao
            )
        from juncao
    ),

    enriquecimento as (
        select
            estabelecimentos.area_programatica,
            estabelecimentos.nome_limpo,
            juncao.*,
            condicoes_ativas.condicoes_ativas
        from normaliza_estado_marcacao as juncao
            left join condicoes_ativas on juncao.id_atendimento = condicoes_ativas.id_atendimento
            left join estabelecimentos on juncao.id_cnes = estabelecimentos.id_cnes
    ),

    -- TEMPORARIO P/ ANONIMIZAR
    anonimizacao as (
        select
            * except (
                nome, cpf, cns, data_nascimento,
                id_agendamento, id_atendimento,
                motivo, plano_observacoes, avaliacao_observacoes, notas_observacoes
            ),
            
            -- Restringe a granularidade da data de nascimento para mês e ano
            FORMAT_DATE('%Y-%m', data_nascimento) AS mes_ano_nascimento
            

        from enriquecimento
    )
select *
from anonimizacao
order by id_cnes, dthr_marcacao