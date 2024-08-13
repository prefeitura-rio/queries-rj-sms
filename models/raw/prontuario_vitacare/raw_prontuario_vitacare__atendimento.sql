{{
    config(
        alias="atendimento",
        materialized="table",
    )
}}

with
---=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--
--  Separação de Atendimentos
---=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--
    bruto_atendimento_eventos_com_repeticao as (
        select * 
        from {{ source("brutos_prontuario_vitacare_staging", "atendimento_eventos") }} 
    ),
    bruto_atendimento_eventos_ranqueados as (
        select
            *,
            row_number() over (partition by source_id order by datalake_loaded_at desc) as rank
        from bruto_atendimento_eventos_com_repeticao
    ),
    bruto_atendimento as (
        select *
        from bruto_atendimento_eventos_ranqueados 
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
                concat(tipo_sms, ' ', nome_complemento) as nome
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
            json_extract_scalar(condicao_json, "$.cod_cid10") as id
        from bruto_atendimento,
            unnest(json_extract_array({{ dict_to_json('data__condicoes') }})) as condicao_json
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
            json_extract_scalar(alergia_json, "$.descricao") as descricao,
        from bruto_atendimento,
            unnest(json_extract_array({{ dict_to_json('data__alergias_anamnese') }})) as alergia_json
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
            replace(json_extract_scalar(prescricoes_json, "$.cod_medicamento"), "-", "") as id,
            upper(json_extract_scalar(prescricoes_json, "$.nome_medicamento")) as nome,
            json_extract_scalar(prescricoes_json, "$.uso_continuado") as uso_continuo
        from bruto_atendimento,
            unnest(json_extract_array({{ dict_to_json('data__prescricoes') }})) as prescricoes_json
    ),
    dim_prescricoes_atribuidas as (
        select 
            fk_atendimento,
            array_agg(
                struct(
                    prescricoes.id,
                    coalesce(materiais.descricao, prescricoes.nome) as nome,
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
            -- Chave
            safe_cast(source_id as string) as id,

            -- Paciente
            dim_paciente.paciente,

            -- Tipo e Subtipo
            safe_cast(
                case 
                    when data__eh_coleta = 'True' then 'Exames Complementares'
                    when data__vacinas != '[]' then 'Vacinação'
                    when data__datahora_marcacao_atendimento = '' then 'Demanda Expontânea'
                    else 'Agendada'
                end as string
            ) as tipo,
            safe_cast(
                case 
                    when data__eh_coleta = 'True' then 'N/A'
                    when data__vacinas != '[]' 
                        then JSON_EXTRACT_SCALAR(JSON_EXTRACT({{dict_to_json('data__vacinas')}}, '$[0]'), '$.nome_vacina')
                    else nullif(data__tipo_consulta,'')
                end as string
            ) as subtipo,

            -- Entrada e Saída
            safe_cast(data__datahora_inicio_atendimento as datetime) as entrada_datahora,
            safe_cast(data__datahora_fim_atendimento as datetime) as saida_datahora,

            -- Motivo e Desfecho
            safe_cast(nullif(data__soap_subjetivo_motivo,'') as string) as motivo_atendimento,
            safe_cast(nullif(data__soap_plano_observacoes,'') as string) as desfecho_atendimento,

            -- Condições
            dim_condicoes_atribuidas.condicoes,

            -- Prescricoes
            dim_prescricoes_atribuidas.prescricoes,

            -- Alergias
            dim_alergias_atribuidas.alergias,

            -- Estabelecimento
            dim_estabelecimento.estabelecimento,

            -- Profissional
            struct(
                dim_profissional.id as id,
                dim_profissional.nome as nome,
                dim_profissional.cpf as cpf,
                dim_profissional.cns as cns,
                safe_cast(
                    case 
                        when data__profissional__cbo_descricao like '%Médic%' then 'Médico(a)'
                        when data__profissional__cbo_descricao like '%Enferm%' then 'Enfermeiro(a)'
                        when data__profissional__cbo_descricao like '%dentista%' then 'Dentista'
                        when data__profissional__cbo_descricao like '%social%' then 'Assistente Social'
                        else data__profissional__cbo_descricao
                    end
                as string) as especialidade
            ) as profissional_saude_responsavel,

            -- Prontuário
            struct(
                safe_cast(source_id as string) as id_atendimento,
                safe_cast('vitacare' as string) as fornecedor
            ) as prontuario,

            -- Metadados
            struct(
                safe_cast(source_updated_at as timestamp) as updated_at,
                safe_cast(datalake_loaded_at as timestamp) as loaded_at,
                safe_cast(current_timestamp() as timestamp) as processed_at,
                safe_cast(
                    (
                        ARRAY_LENGTH(dim_condicoes_atribuidas.condicoes) > 0 and 
                        safe_cast(data__datahora_inicio_atendimento as datetime) is not null and
                        safe_cast(nullif(data__soap_subjetivo_motivo, '') as string) is not null
                    ) as boolean
                ) as tem_informacoes_basicas,
                safe_cast(
                    (
                        dim_paciente.paciente.cpf is not null or
                        dim_paciente.paciente.cns is not null
                    ) as boolean
                ) as tem_identificador_paciente,
                safe_cast(
                    false as boolean
                ) as tem_informacoes_sensiveis
            ) as metadados

        from bruto_atendimento
            inner join dim_paciente
                on bruto_atendimento.patient_cpf = dim_paciente.pk
            inner join dim_estabelecimento
                on bruto_atendimento.data__unidade_cnes = dim_estabelecimento.pk
            inner join dim_profissional
                on bruto_atendimento.data__profissional__cns = dim_profissional.pk
            left join dim_condicoes_atribuidas
                on bruto_atendimento.source_id = dim_condicoes_atribuidas.fk_atendimento
            left join dim_alergias_atribuidas
                on bruto_atendimento.source_id = dim_alergias_atribuidas.fk_atendimento
            left join dim_prescricoes_atribuidas
                on bruto_atendimento.source_id = dim_prescricoes_atribuidas.fk_atendimento
    )
---=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--
--  Finalização
---=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--
select *
from fato_atendimento