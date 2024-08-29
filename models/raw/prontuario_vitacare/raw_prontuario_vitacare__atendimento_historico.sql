{{
    config(
        alias="atendimento_historico",
        materialized="table",
    )
}}

with
    fato_atendimento as (
        select
            acto_id,
            patient_cpf,
            unidade_ap,
            unidade_cnes,
            struct(
                profissional_cns as cns,
                profissional_cpf as cpf,
                profissional_nome as nome,
                profissional_cbo as cbo,
                profissional_cbo_descricao as descricao,
                struct(
                profissional_equipe_nome as nome,
                profissional_equipe_cod_ine as cod_ine
                ) as equipe
            ) as profissional,
            datahora_inicio_atendimento,
            datahora_fim_atendimento,
            datahora_marcacao_atendimento,
            tipo_consulta,
            eh_coleta,
            subjetivo_motivo,
            safe_cast(null as string) as plano_procedimentos_clinicos
            plano_observacoes,
            avaliacao_observacoes,
            objetivo_descricao,
            notas_observacoes,
            datahora_fim_atendimento as updated_at,
            imported_at
        from {{ source("brutos_prontuario_vitacare_staging", "atendimentos_historico") }} 
    ),
    dim_alergias as (
        select
            acto_id,
            array_agg(alergias_anamnese_descricao) as alergias
        from {{ source("brutos_prontuario_vitacare_staging", "alergias_historico") }} 
        group by acto_id
    ),
    dim_condicoes as (
        select
            acto_id,
            array_agg(
                struct(
                cod_cid10,
                estado,
                data_diagnostico
                )
            ) as condicoes
        from {{ source("brutos_prontuario_vitacare_staging", "condicoes_historico") }} 
        group by acto_id
        ),
    dim_encaminhamentos as (
        select
            acto_id,
            array_agg(
                encaminhamento_especialidade
            ) as encaminhamentos
        from {{ source("brutos_prontuario_vitacare_staging", "encaminhamentos_historico") }} 
        group by acto_id
    ),
    dim_indicadores as (
        select
            acto_id,
            array_agg(
                struct(
                indicadores_nome as nome,
                valor
                )
            ) as indicadores
        from {{ source("brutos_prontuario_vitacare_staging", "indicadores_historico") }} 
        group by acto_id
    ),
    dim_exames as (
        select
            acto_id,
            array_agg(
                struct(
                nome_exame,
                cod_exame,
                quantidade,
                material,
                data_solicitacao
                )
            ) as exames_solicitados
        from {{ source("brutos_prontuario_vitacare_staging", "solicitacao_historico") }} 
        group by acto_id
    ),
    dim_vacinas as (
        select
            acto_id,
            array_agg(
                struct(
                nome_vacina,
                cod_vacina,
                dose,
                lote,
                data_aplicacao,
                data_registro,
                estrategia_imunizacao,
                tipo_registro,
                calendario_vacinal_atualizado,
                diff
                )
            ) as vacinas
        from {{ source("brutos_prontuario_vitacare_staging", "vacinas_historico") }} 
        group by acto_id
    ),
    dim_prescricoes as (
        select
            acto_id,
            array_agg(
                struct(
                nome_medicamento,
                cod_medicamento,
                posologia,
                quantidade,
                uso_continuado
                )
            ) as prescricoes
        from {{ source("brutos_prontuario_vitacare_staging", "prescricoes_historico") }} 
        group by acto_id
    ),
    dim_procedimentos as (
        select
            acto_id,
            array_agg(
                procedimento_obs
            ) as procedimentos
        from {{ source("brutos_prontuario_vitacare_staging", "procedimentos_historico") }} 
        group by acto_id
    ),
    atendimentos_eventos_historicos as (
        select 
            atendimentos.*,
            dim_alergias.alergias,
            dim_condicoes.condicoes,
            dim_encaminhamentos.encaminhamentos,
            dim_indicadores.indicadores,
            dim_exames.exames_solicitados,
            dim_vacinas.vacinas,
            dim_prescricoes.prescricoes,
            dim_procedimentos.procedimentos
        from fato_atendimento
            left join dim_alergias using (acto_id)
            left join dim_condicoes using (acto_id)
            left join dim_encaminhamentos using (acto_id)
            left join dim_indicadores using (acto_id)
            left join dim_exames using (acto_id)
            left join dim_vacinas using (acto_id)
            left join dim_prescricoes using (acto_id)
            left join dim_procedimentos using (acto_id)
    )
select
    *
from atendimentos_eventos_historicos