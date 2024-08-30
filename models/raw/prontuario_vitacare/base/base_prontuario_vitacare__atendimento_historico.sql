{{
    config(
        alias="atendimento_historico",
        materialized="table",
    )
}}

with
    dim_equipe as (
        select *
        from {{ source("brutos_prontuario_vitacare_staging", "equipes_historico") }}
    ),

    fato_atendimento as (
        select
            -- PK
            acto_id,
            concat(nullif(unidade_cnes, ''), '.', nullif(acto_id, '')) as gid,

            -- Chaves
            patient_cpf as cpf,
            unidade_cnes as cnes_unidade,

            -- Profissional
            profissional_cns as cns_profissional,
            profissional_cpf as cpf_profissional,
            profissional_nome as nome_profissional,
            profissional_cbo as cbo_profissional,
            profissional_cbo_descricao as cbo_descricao_profissional,
            dim_equipe.codigo as cod_equipe_profissional,
            profissional_equipe_cod_ine as cod_ine_equipe_profissional,
            profissional_equipe_nome as nome_equipe_profissional,

            -- Dados da Consulta
            tipo_consulta as tipo,
            eh_coleta,
            safe_cast(datahora_marcacao_atendimento as datetime) as datahora_marcacao,
            safe_cast(datahora_inicio_atendimento as datetime) as datahora_inicio,
            safe_cast(datahora_fim_atendimento as datetime) as datahora_fim,

            -- Campos Livres
            subjetivo_motivo as soap_subjetivo_motivo,
            objetivo_descricao as soap_objetivo_descricao,
            avaliacao_observacoes as soap_avaliacao_observacoes,
            safe_cast(null as string) as soap_plano_procedimentos_clinicos,
            plano_observacoes as soap_plano_observacoes,
            notas_observacoes as soap_notas_observacoes,

            -- Metadados
            safe_cast(datahora_fim_atendimento as datetime) as updated_at,
            safe_cast(atendimentos.imported_at as datetime) as loaded_at
        from
            {{ source("brutos_prontuario_vitacare_staging", "atendimentos_historico") }} as atendimentos
        left join dim_equipe on atendimentos.profissional_equipe_cod_ine = dim_equipe.n_ine
    ),
    dim_alergias as (
        select
            acto_id,
            array_agg(
                to_json_string(struct(alergias_anamnese_descricao as descricao))
            ) as alergias
        from {{ source("brutos_prontuario_vitacare_staging", "alergias_historico") }}
        group by acto_id
    ), 
    dim_condicoes as (
        select
            acto_id,
            array_agg(
                to_json_string(
                    struct(cod_cid10, "" as cod_ciap2, estado, data_diagnostico)
                )
            ) as condicoes
        from {{ source("brutos_prontuario_vitacare_staging", "condicoes_historico") }}
        group by acto_id
    ),
    dim_encaminhamentos as (
        select
            acto_id,
            array_agg(
                to_json_string(struct(encaminhamento_especialidade as descricao))
            ) as encaminhamentos
        from
            {{
                source(
                    "brutos_prontuario_vitacare_staging", "encaminhamentos_historico"
                )
            }}
        group by acto_id
    ),
    dim_indicadores as (
        select
            acto_id,
            array_agg(
                to_json_string(struct(indicadores_nome as nome, valor))
            ) as indicadores
        from {{ source("brutos_prontuario_vitacare_staging", "indicadores_historico") }}
        group by acto_id
    ),
    dim_exames as (
        select
            acto_id,
            array_agg(
                to_json_string(
                    struct(
                        nome_exame, cod_exame, quantidade, material, data_solicitacao
                    )
                )
            ) as exames_solicitados
        from {{ source("brutos_prontuario_vitacare_staging", "exame_historico") }}
        group by acto_id
    ),
    dim_vacinas as (
        select
            acto_id,
            array_agg(
                to_json_string(
                    struct(
                        nome_vacina,
                        cod_vacina,
                        dose,
                        lote,
                        data_aplicacao as datahora_aplicacao,
                        data_registro as datahora_registro,
                        diff,
                        calendario_vacinal_atualizado,
                        "" as dose_vtc,
                        tipo_registro,
                        estrategia_imunizacao
                    )
                )
            ) as vacinas
        from {{ source("brutos_prontuario_vitacare_staging", "vacinas_historico") }}
        group by acto_id
    ),
    dim_prescricoes as (
        select
            acto_id,
            array_agg(
                to_json_string(
                    struct(
                        nome_medicamento,
                        cod_medicamento,
                        posologia,
                        quantidade,
                        uso_continuado
                    )
                )
            ) as prescricoes
        from {{ source("brutos_prontuario_vitacare_staging", "prescricoes_historico") }}
        group by acto_id
    ),

    atendimentos_eventos_historicos as (
        select
            atendimentos.* except (acto_id, updated_at, loaded_at),

            dim_prescricoes.prescricoes,
            dim_condicoes.condicoes,
            dim_exames.exames_solicitados,
            dim_alergias.alergias as alergias_anamnese,
            dim_vacinas.vacinas,
            dim_indicadores.indicadores,
            dim_encaminhamentos.encaminhamentos,
            -- dim_procedimentos.procedimentos,
            atendimentos.updated_at,
            atendimentos.loaded_at

        from fato_atendimento as atendimentos
        left join dim_alergias using (acto_id)
        left join dim_condicoes using (acto_id)
        left join dim_encaminhamentos using (acto_id)
        left join dim_indicadores using (acto_id)
        left join dim_exames using (acto_id)
        left join dim_vacinas using (acto_id)
        left join dim_prescricoes using (acto_id)
    -- left join dim_procedimentos using (acto_id)
    )

select *
from atendimentos_eventos_historicos
