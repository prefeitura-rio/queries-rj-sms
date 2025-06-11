{{
    config(
        schema="brutos_prontuario_vitacare_historico_staging",
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
        select
            {{ remove_double_quotes('codigo') }} as codigo,
            {{ remove_double_quotes('n_ine') }} as n_ine
        from {{ source("brutos_prontuario_vitacare_historico_staging", "EQUIPES") }}
    ),

    fato_atendimento as (
        select
            -- PK
            replace({{ remove_double_quotes('acto_id') }}, '.0', '') as id_prontuario_local,
            concat(
                nullif({{ remove_double_quotes('atendimentos.id_cnes') }}, ''),
                '.',
                nullif(
                    replace({{ remove_double_quotes('acto_id') }}, '.0', ''), ''
                )
            ) as id_prontuario_global,

            -- Chaves
            nullif({{ remove_double_quotes('patient_cpf') }}, 'NAO TEM') as cpf,
            {{ remove_double_quotes('unidade_cnes') }} as cnes_unidade,

            -- Profissional
            {{ remove_double_quotes('profissional_cns') }} as cns_profissional,
            {{ remove_double_quotes('profissional_cpf') }} as cpf_profissional,
            {{ remove_double_quotes('profissional_nome') }} as nome_profissional,
            {{ remove_double_quotes('profissional_cbo') }} as cbo_profissional,
            {{ remove_double_quotes('profissional_cbo_descricao') }} as cbo_descricao_profissional,
            {{ remove_double_quotes(process_null('dim_equipe.codigo')) }} as cod_equipe_profissional,
            {{ remove_double_quotes(process_null('profissional_equipe_cod_ine')) }} as cod_ine_equipe_profissional,
            {{ remove_double_quotes(process_null('profissional_equipe_nome')) }} as nome_equipe_profissional,

            -- Dados da Consulta
            {{ remove_double_quotes('tipo_consulta') }} as tipo,
            {{ remove_double_quotes('eh_coleta') }} as eh_coleta,
            safe_cast({{ remove_double_quotes('datahora_marcacao_atendimento') }} as datetime) as datahora_marcacao,
            safe_cast({{ remove_double_quotes('datahora_inicio_atendimento') }} as datetime) as datahora_inicio,
            safe_cast({{ remove_double_quotes('datahora_fim_atendimento') }} as datetime) as datahora_fim,

            -- Campos Livres
            safe_cast({{ remove_double_quotes(process_null('subjetivo_motivo')) }} as string) as soap_subjetivo_motivo,
            safe_cast({{ remove_double_quotes(process_null('objetivo_descricao')) }} as string) as soap_objetivo_descricao,
            safe_cast({{ remove_double_quotes(process_null('avaliacao_observacoes')) }} as string) as soap_avaliacao_observacoes,
            safe_cast(null as string) as soap_plano_procedimentos_clinicos,
            safe_cast({{ remove_double_quotes(process_null('plano_observacoes')) }} as string) as soap_plano_observacoes,
            safe_cast({{ remove_double_quotes(process_null('notas_observacoes')) }} as string) as soap_notas_observacoes,

            -- Metadados
            safe_cast({{ remove_double_quotes('datahora_fim_atendimento') }} as datetime) as updated_at,
            extracted_at as loaded_at
        from
            {{ source("brutos_prontuario_vitacare_historico_staging", "ATENDIMENTOS") }}
            as atendimentos
        left join
            dim_equipe on {{ remove_double_quotes('atendimentos.profissional_equipe_cod_ine') }} = dim_equipe.n_ine
            
    ),
    dim_alergias as (
        select
            id_prontuario_global,
            to_json_string(
                array_agg(struct(alergias_anamnese_descricao as descricao))
            ) as alergias
        from
            (
                select
                    concat(nullif({{ remove_double_quotes('id_cnes') }}, ''), '.', nullif(replace({{ remove_double_quotes('acto_id') }}, '.0', ''), '')) as id_prontuario_global,
                    {{ remove_double_quotes('alergias_anamnese_descricao') }} as alergias_anamnese_descricao
                from
                    {{
                        source(
                            "brutos_prontuario_vitacare_historico_staging", "ALERGIAS"
                        )
                    }}
            )
        group by id_prontuario_global
    ),
    dim_condicoes as (
        select
            id_prontuario_global,
            to_json_string(
                array_agg(struct(cod_cid10, "" as cod_ciap2, estado, data_diagnostico))
            ) as condicoes
        from
            (
                select
                    concat(nullif({{ remove_double_quotes('id_cnes') }}, ''), '.', nullif(replace({{ remove_double_quotes('acto_id') }}, '.0', ''), '')) as id_prontuario_global,
                    {{ remove_double_quotes('cod_cid10') }} as cod_cid10,
                    {{ remove_double_quotes('estado') }} as estado,
                    {{ remove_double_quotes('data_diagnostico') }} as data_diagnostico
                from
                    {{
                        source(
                            "brutos_prontuario_vitacare_historico_staging",
                            "CONDICOES",
                        )
                    }}
            )
        group by id_prontuario_global
    ),
    dim_encaminhamentos as (
        select
            id_prontuario_global,
            to_json_string(
                array_agg(struct(encaminhamento_especialidade as descricao))
            ) as encaminhamentos
        from
            (
                select
                    concat(nullif({{ remove_double_quotes('id_cnes') }}, ''), '.', nullif(replace({{ remove_double_quotes('acto_id') }}, '.0', ''), '')) as id_prontuario_global,
                    {{ remove_double_quotes('encaminhamento_especialidade') }} as encaminhamento_especialidade
                from
                    {{
                        source(
                            "brutos_prontuario_vitacare_historico_staging",
                            "ENCAMINHAMENTOS",
                        )
                    }}
            )
        group by id_prontuario_global
    ),
    dim_indicadores as (
        select
            id_prontuario_global,
            to_json_string(
                array_agg(struct(indicadores_nome as nome, valor))
            ) as indicadores
        from
            (
                select
                    concat(nullif({{ remove_double_quotes('id_cnes') }}, ''), '.', nullif(replace({{ remove_double_quotes('acto_id') }}, '.0', ''), '')) as id_prontuario_global,
                    {{ remove_double_quotes('indicadores_nome') }} as indicadores_nome,
                    {{ remove_double_quotes('valor') }} as valor
                from
                    {{
                        source(
                            "brutos_prontuario_vitacare_historico_staging",
                            "INDICADORES",
                        )
                    }}
            )
        group by id_prontuario_global
    ),
    dim_exames as (
        select
            id_prontuario_global,
            to_json_string(
                array_agg(
                    struct(
                        nome_exame, cod_exame, quantidade, material, data_solicitacao
                    )
                )
            ) as exames_solicitados
        from
            (
                select
                    concat(nullif({{ remove_double_quotes('id_cnes') }}, ''), '.', nullif(replace({{ remove_double_quotes('acto_id') }}, '.0', ''), '')) as id_prontuario_global,
                    {{ remove_double_quotes('nome_exame') }} as nome_exame,
                    {{ remove_double_quotes('cod_exame') }} as cod_exame,
                    {{ remove_double_quotes('quantidade') }} as quantidade,
                    {{ remove_double_quotes('material') }} as material,
                    {{ remove_double_quotes('data_solicitacao') }} as data_solicitacao
                from
                    {{
                        source(
                            "brutos_prontuario_vitacare_historico_staging", "SOLICITACAO_EXAMES"
                        )
                    }}
            )
        group by id_prontuario_global
    ),
    dim_vacinas as (
        select
            id_prontuario_global,
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
                select
                    concat(nullif({{ remove_double_quotes('id_cnes') }}, ''), '.', nullif(replace({{ remove_double_quotes('acto_id') }}, '.0', ''), '')) as id_prontuario_global,
                    {{ remove_double_quotes('nome_vacina') }} as nome_vacina,
                    {{ remove_double_quotes('cod_vacina') }} as cod_vacina,
                    {{ remove_double_quotes('dose') }} as dose,
                    {{ remove_double_quotes('lote') }} as lote,
                    {{ remove_double_quotes('data_aplicacao') }} as data_aplicacao,
                    {{ remove_double_quotes('data_registro') }} as data_registro,
                    {{ remove_double_quotes('diff') }} as diff,
                    {{ remove_double_quotes('calendario_vacinal_atualizado') }} as calendario_vacinal_atualizado,
                    {{ remove_double_quotes('tipo_registro') }} as tipo_registro,
                    {{ remove_double_quotes('estrategia_imunizacao') }} as estrategia_imunizacao
                from
                    {{
                        source(
                            "brutos_prontuario_vitacare_historico_staging", "VACINAS"
                        )
                    }}
            )
        group by id_prontuario_global
    ),
    dim_prescricoes as (
        select
            id_prontuario_global,
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
                select
                    concat(nullif({{ remove_double_quotes('id_cnes') }}, ''), '.', nullif(replace({{ remove_double_quotes('acto_id') }}, '.0', ''), '')) as id_prontuario_global,
                    {{ remove_double_quotes('nome_medicamento') }} as nome_medicamento,
                    {{ remove_double_quotes('cod_medicamento') }} as cod_medicamento,
                    {{ remove_double_quotes('posologia') }} as posologia,
                    {{ remove_double_quotes('quantidade') }} as quantidade,
                    {{ remove_double_quotes('uso_continuado') }} as uso_continuado
                from
                    {{
                        source(
                            "brutos_prontuario_vitacare_historico_staging",
                            "PRESCRICOES",
                        )
                    }}
            )
        group by id_prontuario_global
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
            atendimentos.updated_at,
            atendimentos.loaded_at,
            safe_cast(atendimentos.datahora_fim as date) as data_particao

        from fato_atendimento as atendimentos
        left join dim_alergias using (id_prontuario_global)
        left join dim_condicoes using (id_prontuario_global)
        left join dim_encaminhamentos using (id_prontuario_global)
        left join dim_indicadores using (id_prontuario_global)
        left join dim_exames using (id_prontuario_global)
        left join dim_vacinas using (id_prontuario_global)
        left join dim_prescricoes using (id_prontuario_global)
    ),

    final as (
        select
            id_prontuario_local,
            id_prontuario_global,
            {{
                dbt_utils.generate_surrogate_key(
                    [
                        "id_prontuario_global",
                    ]
                )
            }} as id_hci,
            * except (id_prontuario_local, id_prontuario_global)
        from atendimentos_eventos_historicos
    )

select * from final