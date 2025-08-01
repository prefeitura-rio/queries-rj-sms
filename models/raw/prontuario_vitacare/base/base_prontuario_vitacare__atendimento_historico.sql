{{
    config(
        schema="brutos_prontuario_vitacare_staging",
        alias="_base_atendimento_historico",
        materialized="incremental",
        incremental_strategy='merge', 
        unique_key=['id_prontuario_global'],
        cluster_by="cpf",
        partition_by={
            "field": "data_particao",
            "data_type": "date",
            "granularity": "day",
        },
        tags=['weekly']
    )
}}

{% set partitions_to_replace = (
    "date_sub(current_date('America/Sao_Paulo'), interval 10 day)"
) %}

with
    dim_equipe as (
        select
            codigo,
            n_ine
        from {{ ref("raw_prontuario_vitacare_historico__equipe") }}
    ),

    fato_atendimento as (
        select
            -- PK
            id_prontuario_local,
            id_prontuario_global,

            -- Chaves
            nullif(patient_cpf, 'NAO TEM') as cpf,
            id_cnes as cnes_unidade,

            -- Profissional
            profissional_cns as cns_profissional,
            profissional_cpf as cpf_profissional,
            profissional_nome as nome_profissional,
            profissional_cbo as cbo_profissional,
            profissional_cbo_descricao as cbo_descricao_profissional,
            -- dim_equipe.codigo 
            '' as cod_equipe_profissional,
            profissional_equipe_cod_ine as cod_ine_equipe_profissional,
            profissional_equipe_nome as nome_equipe_profissional,

            -- Dados da Consulta
            tipo_consulta as tipo,
            eh_coleta,
            datahora_marcacao_atendimento as datahora_marcacao,
            datahora_inicio_atendimento as datahora_inicio,
            datahora_fim_atendimento as datahora_fim,

            -- Campos Livres
            subjetivo_motivo as soap_subjetivo_motivo,
            objetivo_descricao as soap_objetivo_descricao,
            avaliacao_observacoes as soap_avaliacao_observacoes,
            plano_observacoes as soap_plano_observacoes,
            notas_observacoes as soap_notas_observacoes,

            -- Metadados
            safe_cast(datahora_fim_atendimento as datetime) as updated_at,
            safe_cast(loaded_at as datetime) as loaded_at,
        from
            {{ ref("raw_prontuario_vitacare_historico__acto") }}
            as atendimentos
        left join
            dim_equipe on atendimentos.profissional_equipe_cod_ine = dim_equipe.n_ine
        {% if is_incremental() %} 
        where data_particao >=  {{ partitions_to_replace }}
        {% endif %}
            
    ),
    dim_alergias as (
        select
            id_prontuario_global,
            to_json_string(
                array_agg(struct(alergias_anamnese_descricao as descricao))
            ) as alergias
        from {{ref("raw_prontuario_vitacare_historico__alergia") }}
        {% if is_incremental() %} 
        where data_particao >= {{ partitions_to_replace }} 
        {% endif %}
        group by id_prontuario_global
    ),
    dim_condicoes as (
        select
            id_prontuario_global,
            to_json_string(
                array_agg(struct(cod_cid10, "" as cod_ciap2, estado, data_diagnostico))
            ) as condicoes
        from {{ ref("raw_prontuario_vitacare_historico__condicao") }}
        {% if is_incremental() %} 
        where data_particao >= {{ partitions_to_replace }} 
        {% endif %}
        group by id_prontuario_global
    ),
    dim_encaminhamentos as (
        select
            id_prontuario_global,
            to_json_string(
                array_agg(struct(encaminhamento_especialidade as descricao))
            ) as encaminhamentos
        from{{ ref("raw_prontuario_vitacare_historico__encaminhamento")}}
        {% if is_incremental() %} 
        where data_particao >= {{ partitions_to_replace }} 
        {% endif %}
        group by id_prontuario_global
    ),
    dim_indicadores as (
        select
            id_prontuario_global,
            to_json_string(
                array_agg(struct(indicadores_nome as nome, valor))
            ) as indicadores
        from {{ ref("raw_prontuario_vitacare_historico__indicador")}}
        {% if is_incremental() %} 
        where data_particao >= {{ partitions_to_replace }} 
        {% endif %}
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
        from {{ ref("raw_prontuario_vitacare_historico__solicitacaoexame") }}
        {% if is_incremental() %} 
        where data_particao >=  {{ partitions_to_replace }} 
        {% endif %}
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
        from {{ ref("raw_prontuario_vitacare_historico__vacina") }}
        {% if is_incremental() %} 
        where data_particao >= {{ partitions_to_replace }} 
        {% endif %}
        group by id_prontuario_global
    ),
    dim_prescricoes as (
        select
            id_prontuario_global,
            to_json_string(
                array_agg(
                    struct(
                        id_medicamento as cod_medicamento,
                        medicamento_nome as nome_medicamento,
                        posologia,
                        quantidade,
                        uso_continuado
                    )
                )
            ) as prescricoes
        from {{ ref("raw_prontuario_vitacare_historico__prescricao") }}
        {% if is_incremental() %} 
        where data_particao >= {{ partitions_to_replace }} 
        {% endif %}
        group by id_prontuario_global
    ),
    dim_procedimentos_clinicos as (
        select
            id_prontuario_global,
            to_json_string(
                array_agg(
                    struct(
                        co_procedimento,
                        no_procedimento AS procedimento_clinico
                    )
                )
            ) as procedimentos_clinicos
        from {{ ref("raw_prontuario_vitacare_historico__procedimentos_clinicos") }}
        {% if is_incremental() %} 
        where data_particao >= {{ partitions_to_replace }} 
        {% endif %}
        group by id_prontuario_global
    ),

    atendimentos_eventos_historicos as (
        select
            atendimentos.* except (updated_at, loaded_at),


            dim_procedimentos_clinicos.procedimentos_clinicos AS soap_plano_procedimentos_clinicos,
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
        left join dim_procedimentos_clinicos using (id_prontuario_global)
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

select *
from final