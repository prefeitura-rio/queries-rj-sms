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
            cpf,
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
            cns,
            struct(
                id_profissional_sus as id,
                cns,
                cpf,
                nome
            ) as profissional_saude_responsavel
        from {{ ref("dim_profissional_saude") }}
    ),
---=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--
--  DIM: Estabelecimento
---=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--
    dim_estabelecimento as (
        select
            id_cnes as cnes,
            struct(
                id_cnes,
                nome_limpo as nome,
                tipo_sms_simplificado as estabelecimento_tipo
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
            source_id as id_atendimento,
            JSON_EXTRACT_SCALAR(condicao_json, "$.cod_cid10") as id
        from latests_events,
            UNNEST(JSON_EXTRACT_ARRAY({{ dict_to_json('data__condicoes') }})) as condicao_json
    ),
    dim_condicao as (
        select 
            id_atendimento,
            array_agg(
                struct(
                    condicoes.id as id,
                    cid_descricao.descricao as descricao
                )
            ) as condicoes
        from condicoes
            left join cid_descricao
                on condicoes.id = cid_descricao.codigo_cid
        group by id_atendimento
    ),
---=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--
--  DIM: Alergias
---=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--
    alergias as (
        select 
            source_id as id_atendimento,
            JSON_EXTRACT_SCALAR(alergia_json, "$.descricao") as descricao,
        from latests_events,
            UNNEST(JSON_EXTRACT_ARRAY({{ dict_to_json('data__alergias_anamnese') }})) as alergia_json
    ),
    dim_alergia as (
        select 
            id_atendimento,
            array_agg(
                alergias.descricao
            ) as alergias
        from alergias
        group by id_atendimento
    ),
---=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--
--  DIM: Medicamento Prescrito
---=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--
    prescricoes as (
        select 
            source_id as id_atendimento,
            JSON_EXTRACT_SCALAR(prescricoes_json, "$.cod_medicamento") as id,
            JSON_EXTRACT_SCALAR(prescricoes_json, "$.nome_medicamento") as nome,
            JSON_EXTRACT_SCALAR(prescricoes_json, "$.uso_continuado") as uso_continuo,
            safe_cast(null as string) as frequencia,
            safe_cast(null as string) as dose,
            safe_cast(null as string) as duracao,
        from latests_events,
            UNNEST(JSON_EXTRACT_ARRAY({{ dict_to_json('data__prescricoes') }})) as prescricoes_json
    ),
    dim_prescricoes as (
        select 
            id_atendimento,
            array_agg(
                struct(
                    prescricoes.id,
                    prescricoes.nome,
                    prescricoes.uso_continuo,
                    prescricoes.frequencia,
                    prescricoes.dose,
                    prescricoes.duracao
                )
            ) as prescricoes
        from prescricoes
        group by id_atendimento
    ),
---=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--
--  FATO: Atendimento
---=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--
    fato_atendimento as (
        select 
            -- PK
            safe_cast(source_id as string) as id,

            -- FK
            safe_cast(patient_cpf as string) as cpf_paciente,
            safe_cast(data__unidade_cnes as string) as cnes_unidade,
            safe_cast(data__profissional__cns as string) as cns_profissional,

            -- Informações Básicas do Atendimento
            safe_cast(
                CASE 
                    WHEN data__eh_coleta='True' THEN 'Exames Complementares'
                    WHEN data__datahora_marcacao_atendimento='' THEN 'Demanda Expontânea'
                    ELSE 'Agendada'
                END as string
            ) as tipo_atendimento,
            safe_cast(
                CASE 
                    WHEN data__eh_coleta='True' THEN 'N/A'
                    ELSE nullif(data__tipo_consulta,'')
                END as string
            ) as subtipo_atendimento,
            safe_cast(nullif(data__soap_subjetivo_motivo,'') as string) as motivo_atendimento,
            safe_cast(nullif(data__soap_plano_observacoes,'') as string) as desfecho_atendimento,
            safe_cast(data__datahora_inicio_atendimento as timestamp) as datahora_atendimento_inicio,
            safe_cast(data__datahora_fim_atendimento as timestamp) as datahora_atendimento_fim,

            -- Prontuario
            struct(
                safe_cast(source_id as string) as id_atendimento,
                safe_cast('vitacare' as string) as fornecedor
            ) as prontuario,

            -- Metadados
            struct(
                safe_cast(source_updated_at as timestamp) as updated_at,
                safe_cast(null as timestamp) as imported_at
            ) as metadados,
        from latests_events
    )
---=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--
--  Finalização
---=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--
select 
    *
    except(id, id_atendimento, cpf_paciente, cpf, cnes_unidade, cnes, cns_profissional, cns)
from fato_atendimento
    inner join dim_paciente
        on fato_atendimento.cpf_paciente = dim_paciente.cpf
    inner join dim_estabelecimento
        on fato_atendimento.cnes_unidade = dim_estabelecimento.cnes
    inner join dim_profissional
        on fato_atendimento.cns_profissional = dim_profissional.cns
    left join dim_condicao
        on fato_atendimento.id = dim_condicao.id_atendimento
    left join dim_alergia
        on fato_atendimento.id = dim_alergia.id_atendimento
    left join dim_prescricoes
        on fato_atendimento.id = dim_prescricoes.id_atendimento