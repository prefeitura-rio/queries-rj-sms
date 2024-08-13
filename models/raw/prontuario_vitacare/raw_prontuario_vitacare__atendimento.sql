{{
    config(
        alias="atendimento",
        materialized="table",
    )
}}

with
---=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--
--  Event Setup
---=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--
    events as (
        select * 
        from {{ source("brutos_prontuario_vitacare_staging", "atendimento_eventos") }} 
    ),
    ranked_events as (
        select
            *,
            row_number() over (partition by source_id order by source_updated_at desc) as rank
        from events
    ),
    latests_events as (
        select *
        from ranked_events 
        where rank = 1
    ),
---=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--
--  DIM: Paciente
---=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--
    dim_paciente as (
        select
            cpf as pk,
            struct(
                id as id_prontuario,
                cpf,
                cns
            ) as paciente
        from {{ ref('raw_prontuario_vitacare__paciente') }}
    ),
---=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--
--  DIM: Profissional
---=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--
    dim_profissional as (
        select
            cns as pk,
            id_profissional_sus as id,
            cns,
            cpf,
            nome,
        from {{ ref("dim_profissional_saude") }}
    ),
---=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--
--  DIM: Estabelecimento
---=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--
    dim_estabelecimento as (
        select
            id_cnes as pk,
            struct(
                id_cnes,
                tipo_sms as estabelecimento_tipo,
                nome_complemento as nome
            ) as estabelecimento
        from {{ ref("dim_estabelecimento") }}
    ),
---=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--
--  DIM: Condições
---=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--
    cid_descricao as (
        select *
        from {{ ref("raw_datasus__cid10") }}
    ),
    condicoes as (
        select 
            source_id as fk_atendimento,
            JSON_EXTRACT_SCALAR(condicao_json, "$.cod_cid10") as id
        from latests_events,
            UNNEST(JSON_EXTRACT_ARRAY({{ dict_to_json('data__condicoes') }})) as condicao_json
    ),
    dim_condicoes_atribuidas as (
        select 
            fk_atendimento,
            array_agg(
                struct(
                    condicoes.id as id,
                    cid_descricao.descricao as descricao
                )
            ) as condicoes
        from condicoes
            left join cid_descricao
                on condicoes.id = cid_descricao.codigo_cid
        group by fk_atendimento
    ),
---=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--
--  DIM: Alergias
---=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--
    alergias as (
        select 
            source_id as fk_atendimento,
            JSON_EXTRACT_SCALAR(alergia_json, "$.descricao") as descricao,
        from latests_events,
            UNNEST(JSON_EXTRACT_ARRAY({{ dict_to_json('data__alergias_anamnese') }})) as alergia_json
    ),
    dim_alergias_atribuidas as (
        select 
            fk_atendimento,
            array_agg(
                alergias.descricao
            ) as alergias
        from alergias
        group by fk_atendimento
    ),
---=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--
--  DIM: Medicamento Prescrito
---=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--
    materiais as (
        select id_material, descricao, concentracao
        from {{ ref("dim_material") }}
    ),
    prescricoes as (
        select 
            source_id as fk_atendimento,
            REPLACE(JSON_EXTRACT_SCALAR(prescricoes_json, "$.cod_medicamento"), "-", "") as id,
            JSON_EXTRACT_SCALAR(prescricoes_json, "$.nome_medicamento") as nome,
            JSON_EXTRACT_SCALAR(prescricoes_json, "$.uso_continuado") as uso_continuo
        from latests_events,
            UNNEST(JSON_EXTRACT_ARRAY({{ dict_to_json('data__prescricoes') }})) as prescricoes_json
    ),
    dim_prescricoes_atribuidas as (
        select 
            fk_atendimento,
            array_agg(
                struct(
                    prescricoes.id,
                    materiais.descricao as nome,
                    materiais.concentracao,
                    prescricoes.uso_continuo
                )
            ) as prescricoes
        from prescricoes
            left join materiais
                on prescricoes.id = materiais.id_material
        group by fk_atendimento
    ),
---=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--
--  FATO: Atendimento
---=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--
    fato_atendimento as (
        select 
            -- PK
            safe_cast(source_id as string) as id,

            -- FK
            safe_cast(patient_cpf as string) as fk_paciente,
            safe_cast(data__unidade_cnes as string) as fk_unidade,
            safe_cast(data__profissional__cns as string) as fk_profissional,

            -- Especialidade do Profissional
            safe_cast(
                CASE WHEN data__profissional__cbo_descricao like '%Médic%' THEN 'Médico(a)'
                WHEN data__profissional__cbo_descricao like '%Enferm%' THEN 'Enfermeiro(a)'
                WHEN data__profissional__cbo_descricao like '%dentista%' THEN 'Dentista'
                WHEN data__profissional__cbo_descricao like '%social%' THEN 'Assistente Social'
                ELSE data__profissional__cbo_descricao
                END
            as string) as profissional_especialidade,

            -- Tipo e Subtipo de Atendimento
            safe_cast(
                CASE 
                    WHEN data__eh_coleta = 'True' THEN 'Exames Complementares'
                    WHEN data__vacinas != '[]' THEN 'Vacinação'
                    WHEN data__datahora_marcacao_atendimento = '' THEN 'Demanda Expontânea'
                    ELSE 'Agendada'
                END as string
            ) as tipo,
            safe_cast(
                CASE 
                    WHEN data__eh_coleta = 'True' THEN 'N/A'
                    WHEN data__vacinas != '[]' 
                        THEN JSON_EXTRACT_SCALAR(JSON_EXTRACT({{dict_to_json('data__vacinas')}}, '$[0]'), '$.nome_vacina')
                    ELSE nullif(data__tipo_consulta,'')
                END as string
            ) as subtipo,

            -- Campos Textuais
            safe_cast(nullif(data__soap_subjetivo_motivo,'') as string) as motivo_atendimento,
            safe_cast(nullif(data__soap_plano_observacoes,'') as string) as desfecho_atendimento,

            -- Timestamps
            safe_cast(data__datahora_inicio_atendimento as datetime) as entrada_datahora,
            safe_cast(data__datahora_fim_atendimento as datetime) as saida_datahora,

            -- Prontuario
            struct(
                safe_cast(source_id as string) as id_atendimento,
                safe_cast('vitacare' as string) as fornecedor
            ) as prontuario,

            -- Metadados
            struct(
                safe_cast(source_updated_at as timestamp) as updated_at,
                safe_cast(datalake_loaded_at as timestamp) as loaded_at,
                safe_cast(current_timestamp() as timestamp) as processed_at
            ) as metadados,
        from latests_events
    )
---=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--
--  Finalização
---=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--
select 
    dim_paciente.paciente,
    fato_atendimento.tipo,
    fato_atendimento.subtipo,
    fato_atendimento.entrada_datahora,
    fato_atendimento.saida_datahora,
    fato_atendimento.motivo_atendimento,
    fato_atendimento.desfecho_atendimento,
    dim_condicoes_atribuidas.condicoes as condicoes,
    dim_prescricoes_atribuidas.prescricoes as prescricoes,
    dim_alergias_atribuidas.alergias as alergias,
    dim_estabelecimento.estabelecimento,
    struct(
        dim_profissional.id,
        dim_profissional.nome,
        dim_profissional.cpf,
        dim_profissional.cns,
        profissional_especialidade as especialidade
    ) as profissional_saude_responsavel,
    fato_atendimento.prontuario,
    fato_atendimento.metadados
from fato_atendimento
    inner join dim_paciente
        on fato_atendimento.fk_paciente = dim_paciente.pk
    inner join dim_estabelecimento
        on fato_atendimento.fk_unidade = dim_estabelecimento.pk
    inner join dim_profissional
        on fato_atendimento.fk_profissional = dim_profissional.pk
    left join dim_condicoes_atribuidas
        on fato_atendimento.id = dim_condicoes_atribuidas.fk_atendimento
    left join dim_alergias_atribuidas
        on fato_atendimento.id = dim_alergias_atribuidas.fk_atendimento
    left join dim_prescricoes_atribuidas
        on fato_atendimento.id = dim_prescricoes_atribuidas.fk_atendimento