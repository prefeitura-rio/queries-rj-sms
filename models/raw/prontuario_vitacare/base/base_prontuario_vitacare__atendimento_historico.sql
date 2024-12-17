{{
    config(
        schema="brutos_prontuario_vitacare_staging",
        alias="_base_atendimento_historico",
        materialized="table",
        partition_by={
            "field": "data_particao",
            "data_type": "date",
            "granularity": "day",
        },
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
            concat(
                nullif(atendimentos.id_cnes, ''),
                '.',
                nullif(
                    replace(acto_id, '.0', ''), ''
                )
            ) as id,

            -- Chaves
            nullif(patient_cpf, 'NAO TEM') as cpf,
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
            safe_cast({{ process_null('subjetivo_motivo') }} as string) as soap_subjetivo_motivo,
            safe_cast({{ process_null('objetivo_descricao') }}  as string) as soap_objetivo_descricao,
            safe_cast({{ process_null('avaliacao_observacoes') }} as string) as soap_avaliacao_observacoes,
            safe_cast(null as string) as soap_plano_procedimentos_clinicos,
            safe_cast({{ process_null('plano_observacoes') }}  as string) as soap_plano_observacoes,
            safe_cast({{ process_null('notas_observacoes') }}  as string) as soap_notas_observacoes,

            -- Metadados
            safe_cast(datahora_fim_atendimento as datetime) as updated_at,
            safe_cast(atendimentos.datalake_imported_at as datetime) as loaded_at
        from
            {{ source("brutos_prontuario_vitacare_staging", "atendimentos_historico") }}
            as atendimentos
        left join
            dim_equipe on atendimentos.profissional_equipe_cod_ine = dim_equipe.n_ine
    ),
    dim_alergias as (
        select
            id,
            to_json_string(
                array_agg(struct(alergias_anamnese_descricao as descricao))
            ) as alergias
        from
            (
                select *, concat(nullif(id_cnes, ''), '.', nullif(acto_id, '')) as id
                from
                    {{
                        source(
                            "brutos_prontuario_vitacare_staging", "alergias_historico"
                        )
                    }}
            )
        group by id
    ),
    dim_condicoes as (
        select
            id,
            to_json_string(
                array_agg(struct(cod_cid10, "" as cod_ciap2, estado, data_diagnostico))
            ) as condicoes
        from
            (
                select *, concat(nullif(id_cnes, ''), '.', nullif(acto_id, '')) as id
                from
                    {{
                        source(
                            "brutos_prontuario_vitacare_staging",
                            "condicoes_historico",
                        )
                    }}
            )
        group by id
    ),
    dim_encaminhamentos as (
        select
            id,
            to_json_string(
                array_agg(struct(encaminhamento_especialidade as descricao))
            ) as encaminhamentos
        from
            (
                select *, concat(nullif(id_cnes, ''), '.', nullif(acto_id, '')) as id
                from
                    {{
                        source(
                            "brutos_prontuario_vitacare_staging",
                            "encaminhamentos_historico",
                        )
                    }}
            )
        group by id
    ),
    dim_indicadores as (
        select
            id,
            to_json_string(
                array_agg(struct(indicadores_nome as nome, valor))
            ) as indicadores
        from
            (
                select *, concat(nullif(id_cnes, ''), '.', nullif(acto_id, '')) as id
                from
                    {{
                        source(
                            "brutos_prontuario_vitacare_staging",
                            "indicadores_historico",
                        )
                    }}
            )
        group by id
    ),
    dim_exames as (
        select
            id,
            to_json_string(
                array_agg(
                    struct(
                        nome_exame, cod_exame, quantidade, material, data_solicitacao
                    )
                )
            ) as exames_solicitados
        from
            (
                select *, concat(nullif(id_cnes, ''), '.', nullif(acto_id, '')) as id
                from
                    {{
                        source(
                            "brutos_prontuario_vitacare_staging", "solicitacao_exame_historico"
                        )
                    }}
            )
        group by id
    ),
    dim_vacinas as (
        select
            id,
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
                select *, concat(nullif(id_cnes, ''), '.', nullif(acto_id, '')) as id
                from
                    {{
                        source(
                            "brutos_prontuario_vitacare_staging", "vacinas_historico"
                        )
                    }}
            )
        group by id
    ),
    dim_prescricoes as (
        select
            id,
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
                select *, concat(nullif(id_cnes, ''), '.', nullif(acto_id, '')) as id,
                from
                    {{
                        source(
                            "brutos_prontuario_vitacare_staging",
                            "prescricoes_historico",
                        )
                    }}
            )
        group by id
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
            safe_cast(atendimentos.datahora_fim as date) as data_particao,

        from fato_atendimento as atendimentos
        left join dim_alergias using (id)
        left join dim_condicoes using (id)
        left join dim_encaminhamentos using (id)
        left join dim_indicadores using (id)
        left join dim_exames using (id)
        left join dim_vacinas using (id)
        left join dim_prescricoes using (id)
    -- left join dim_procedimentos using (acto_id)
    ),

    final as (
        select id,
            {{
                dbt_utils.generate_surrogate_key(
                    [
                        "id",
                    ]
                )
            }} as id_hci,
            * except (id),
        from atendimentos_eventos_historicos
    )

select * from final

