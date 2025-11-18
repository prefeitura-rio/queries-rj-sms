{{
  config(
    schema="projeto_teleconsulta",
    alias="teleconsultas",
  )
}}

with

    condicoes_ativas as (
        select
            id_prontuario_global as id_atendimento,
            array_agg(cod_cid10) as condicoes_ativas
        from {{ ref("raw_prontuario_vitacare_historico__condicao") }}
        where estado = 'ATIVO'
        group by 1
    ),

    agendamentos_tele as (
        select
            age.id_global as id_agendamento,
            cast(null as string) as id_atendimento,
            age.id_cnes,

            upper(cad.nome) as nome,
            cad.cpf,
            cad.cns,
            cad.sexo,
            cad.raca_cor,
            cad.bairro as bairro_residencia,
            cad.id_cnes as id_unidade_referencia,
            cad.equipe,
            cad.ine_equipe,

            upper(prof.profissional_nome) as profissional_nome,
            datahora_agendamento as dthr_marcacao,
            safe_cast(null as datetime) as dthr_inicio_atendimento,
            safe_cast(null as datetime) as dthr_fim_atendimento,

            cast(null as string) as eh_coleta,

            estado_marcacao,
            motivo,

            cast(null as string) as subjetivo_motivo,
            cast(null as string) as plano_observacoes,
            cast(null as string) as avaliacao_observacoes,
            cast(null as string) as notas_observacoes
        from {{ ref("raw_prontuario_vitacare_historico__agendamento") }} age
            left join {{ ref("raw_prontuario_vitacare_historico__profissional") }} prof on prof.id_global = age.id_profissional
            left join {{ ref("raw_prontuario_vitacare_historico__cadastro") }} cad on cad.id_global = age.id_cadastro
        where
            age.id_cnes in (
                '5476607', -- CF ADIB JANETE
                '9442251' -- CMS JEREMIAS MORAES
            )
            and tipo_atendimento = 'TELECONSULTA'
    ),
    atendimentos_tele as (
        select
            acto.id_global as id_atendimento,
            cast(null as string) as id_agendamento,
            acto.id_cnes,

            upper(cad.nome) as nome,
            cad.cpf,
            cad.cns,
            cad.sexo,
            cad.raca_cor,
            cad.bairro as bairro_residencia,
            cad.id_cnes as id_unidade_referencia,
            cad.equipe,
            cad.ine_equipe,

            upper(profissional_nome) as profissional_nome,
            datahora_marcacao_atendimento as dthr_marcacao,
            safe_cast(datahora_inicio_atendimento as datetime) as dthr_inicio_atendimento,
            safe_cast(datahora_fim_atendimento as datetime) as dthr_fim_atendimento,

            eh_coleta,

            'EXECUTADO' as estado_marcacao,
            'N/A' as motivo,

            subjetivo_motivo,
            plano_observacoes,
            avaliacao_observacoes,
            notas_observacoes
        from
            {{ ref("raw_prontuario_vitacare_historico__acto") }} acto
            left join {{ ref("raw_prontuario_vitacare_historico__cadastro") }} cad on cad.id_global = acto.id_cadastro
        where
            acto.id_cnes in (
                '5476607', -- CF ADIB JANETE
                '9442251' -- CMS JEREMIAS MORAES
            )
            and tipo_atendimento = 'TELECONSULTA'
    ),
    juncao as (
        select id_agendamento, id_atendimento, id_cnes, nome, cpf, cns, sexo, raca_cor, 
            bairro_residencia, id_unidade_referencia, equipe, ine_equipe, profissional_nome, dthr_marcacao, dthr_inicio_atendimento, dthr_fim_atendimento, 
            estado_marcacao, motivo, subjetivo_motivo, plano_observacoes, avaliacao_observacoes, notas_observacoes
        from agendamentos_tele

        union all

        select id_agendamento, id_atendimento, id_cnes, nome, cpf, cns, sexo, raca_cor, 
            bairro_residencia, id_unidade_referencia, equipe, ine_equipe, profissional_nome, dthr_marcacao, dthr_inicio_atendimento, dthr_fim_atendimento, 
            estado_marcacao, motivo, subjetivo_motivo, plano_observacoes, avaliacao_observacoes, notas_observacoes
        from atendimentos_tele
    ),
    condicoes_ativas_join as (
        select
            juncao.*,
            condicoes_ativas.condicoes_ativas
        from juncao
            left join condicoes_ativas on juncao.id_atendimento = condicoes_ativas.id_atendimento
    ),

    -- TEMPORARIO P/ ANONIMIZAR
    anonimizacao as (
        select
            * except (
                nome, cpf, cns, 
                id_agendamento, id_atendimento,
                motivo, subjetivo_motivo, plano_observacoes, avaliacao_observacoes, notas_observacoes
            )
        from condicoes_ativas_join
    )
select *
from anonimizacao
order by id_cnes, dthr_marcacao