{{
    config(
        alias="_base_atendimento_historico",
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
            concat(nullif(atendimentos.id_cnes, ''), '.', nullif(atendimentos.acto_id, '')) as gid,

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
            {{ source("brutos_prontuario_vitacare_staging", "atendimentos_historico") }}
            as atendimentos
        left join
            dim_equipe on atendimentos.profissional_equipe_cod_ine = dim_equipe.n_ine
    ),
    dim_alergias as (
        select
            gid,
            to_json_string(
                array_agg(struct(alergias_anamnese_descricao as descricao))
            ) as alergias
        from
            (
                select *, concat(nullif(id_cnes, ''), '.', nullif(acto_id, '')) as gid
                from
                    {{
                        source(
                            "brutos_prontuario_vitacare_staging", "alergias_historico"
                        )
                    }}
            )
        group by gid
    ),
    dim_condicoes as (
        select
            gid,
            to_json_string(
                array_agg(struct(cod_cid10, "" as cod_ciap2, estado, data_diagnostico))
            ) as condicoes
        from
            (
                select *, concat(nullif(id_cnes, ''), '.', nullif(acto_id, '')) as gid
                from
                    {{
                        source(
                            "brutos_prontuario_vitacare_staging",
                            "condicoes_historico",
                        )
                    }}
            )
        group by gid
    ),
    dim_encaminhamentos as (
        select
            gid,
            to_json_string(
                array_agg(struct(encaminhamento_especialidade as descricao))
            ) as encaminhamentos
        from
            (
                select *, concat(nullif(id_cnes, ''), '.', nullif(acto_id, '')) as gid
                from
                    {{
                        source(
                            "brutos_prontuario_vitacare_staging",
                            "encaminhamentos_historico",
                        )
                    }}
            )
        group by gid
    ),
    dim_indicadores as (
        select
            gid,
            to_json_string(
                array_agg(struct(indicadores_nome as nome, valor))
            ) as indicadores
        from
            (
                select *, concat(nullif(id_cnes, ''), '.', nullif(acto_id, '')) as gid
                from
                    {{
                        source(
                            "brutos_prontuario_vitacare_staging",
                            "indicadores_historico",
                        )
                    }}
            )
        group by gid
    ),
    dim_exames as (
        select
            gid,
            to_json_string(
                array_agg(
                    struct(
                        nome_exame, cod_exame, quantidade, material, data_solicitacao
                    )
                )
            ) as exames_solicitados
        from
            (
                select *, concat(nullif(id_cnes, ''), '.', nullif(acto_id, '')) as gid
                from
                    {{
                        source(
                            "brutos_prontuario_vitacare_staging", "exame_historico"
                        )
                    }}
            )
        group by gid
    ),
    dim_vacinas as (
        select
            gid,
            to_json_string(
                array_agg(
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
        from
            (
                select *, concat(nullif(id_cnes, ''), '.', nullif(acto_id, '')) as gid
                from
                    {{
                        source(
                            "brutos_prontuario_vitacare_staging", "vacinas_historico"
                        )
                    }}
            )
        group by gid
    ),
    dim_prescricoes as (
        select
            gid,
            to_json_string(
                array_agg(
                    struct(
                        nome_medicamento,
                        cod_medicamento,
                        posologia,
                        quantidade,
                        uso_continuado
                    )
                )
            ) as prescricoes
        from
            (
                select *, concat(nullif(id_cnes, ''), '.', nullif(acto_id, '')) as gid,
                from
                    {{
                        source(
                            "brutos_prontuario_vitacare_staging",
                            "prescricoes_historico",
                        )
                    }}
            )
        group by gid
    ),

    atendimentos_eventos_historicos as (
        select
            atendimentos.* except (updated_at, loaded_at),

            dim_prescricoes.prescricoes,
            dim_condicoes.condicoes,
            dim_exames.exames_solicitados,
            dim_alergias.alergias as alergias_anamnese,
            dim_vacinas.vacinas,
            dim_indicadores.indicadores,
            dim_encaminhamentos.encaminhamentos,
            -- dim_procedimentos.procedimentos,
            atendimentos.updated_at,
            atendimentos.loaded_at,
            safe_cast(atendimentos.datahora_inicio as date) as data_particao,

        from fato_atendimento as atendimentos
        left join dim_alergias using (gid)
        left join dim_condicoes using (gid)
        left join dim_encaminhamentos using (gid)
        left join dim_indicadores using (gid)
        left join dim_exames using (gid)
        left join dim_vacinas using (gid)
        left join dim_prescricoes using (gid)
    -- left join dim_procedimentos using (acto_id)
    )

select * from atendimentos_eventos_historicos

