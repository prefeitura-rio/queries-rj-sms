{{
    config(
        schema="intermediario_historico_clinico",
        alias="episodio_assistencial_vitacare",
        materialized="incremental",
        incremental_strategy="insert_overwrite",
        partition_by={
            "field": "data_particao",
            "data_type": "date",
            "granularity": "day",
        },
    )
}}


{% set partitions_to_replace = [
    "current_date('America/Sao_Paulo')",
    "date_sub(current_date('America/Sao_Paulo'), interval 1 day)",
] %}


with
    -- -=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--
    -- Separação de Atendimentos
    -- -=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--
    bruto_atendimento as (
        select * from {{ ref("raw_prontuario_vitacare__atendimento") }}
    -- where data_particao = "2024-08-01"
    ),
    -- -=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--
    -- DIM: Paciente
    -- -=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--
    dim_paciente as (
        select
            cpf as pk,
            struct(
                paciente_merged.cpf,
                paciente_merged.cns,
                paciente_merged.dados.data_nascimento
            ) as paciente
        from {{ ref("mart_historico_clinico__paciente") }} as paciente_merged
    ),
    -- -=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--
    -- DIM: Profissional
    -- -=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--
    dim_profissional as (
        select cns as pk, id_profissional_sus as id, cns, nome, cpf,
        from {{ ref("dim_profissional_saude") }}
        qualify row_number() over (partition by cpf order by id desc) = 1
    ),
    -- -=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--
    -- DIM: Estabelecimento
    -- -=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--
    dim_estabelecimento as (
        select
            id_cnes as pk,
            struct(
                id_cnes,
                {{ proper_estabelecimento("nome_limpo") }} as nome,
                tipo_sms as estabelecimento_tipo
            ) as estabelecimento
        from {{ ref("dim_estabelecimento") }}
    ),
    -- -=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--
    -- DIM: Condições
    -- -=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--
    cid_descricao as (select * from {{ ref("raw_datasus__cid10") }}),
    condicoes as (
        select
            gid as fk_atendimento,
            json_extract_scalar(condicao_json, "$.cod_cid10") as id
        from bruto_atendimento, unnest(json_extract_array(condicoes)) as condicao_json
    ),
    dim_condicoes_atribuidas as (
        select
            fk_atendimento,
            array_agg(
                struct(condicoes.id as id, cid_descricao.descricao as descricao)
            ) as condicoes
        from condicoes
        left join cid_descricao on condicoes.id = cid_descricao.codigo_cid
        group by fk_atendimento
    ),
    -- -=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--
    -- DIM: Medicamento Prescrito
    -- -=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--
    materiais as (
        select id_material, descricao, concentracao from {{ ref("dim_material") }}
    ),
    prescricoes as (
        select
            gid as fk_atendimento,
            replace(
                json_extract_scalar(prescricoes_json, "$.cod_medicamento"), "-", ""
            ) as id,
            upper(json_extract_scalar(prescricoes_json, "$.nome_medicamento")) as nome,
            json_extract_scalar(prescricoes_json, "$.uso_continuado") as uso_continuo
        from
            bruto_atendimento,
            unnest(json_extract_array(prescricoes)) as prescricoes_json
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
        left join materiais on prescricoes.id = materiais.id_material
        group by fk_atendimento
    ),
    -- -=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--
    -- FATO: Atendimento
    -- -=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--
    fato_atendimento as (
        select
            gid as id,

            -- Paciente
            dim_paciente.paciente,

            -- Tipo e Subtipo
            safe_cast(
                case
                    when eh_coleta = 'True'
                    then 'Exames Complementares'
                    when vacinas != '[]'
                    then 'Vacinação'
                    when datahora_marcacao is null
                    then 'Demanda Expontânea'
                    else 'Agendada'
                end as string
            ) as tipo,
            safe_cast(
                case
                    when eh_coleta = 'True'
                    then 'N/A'
                    when vacinas != '[]'
                    then
                        json_extract_scalar(
                            json_extract(vacinas, '$[0]'), '$.nome_vacina'
                        )
                    else nullif(tipo, '')
                end as string
            ) as subtipo,

            -- Entrada e Saída
            safe_cast(datahora_inicio as datetime) as entrada_datahora,
            safe_cast(datahora_fim as datetime) as saida_datahora,

            -- Motivo e Desfecho
            upper(soap_subjetivo_motivo) as motivo_atendimento,
            upper(soap_plano_observacoes) as desfecho_atendimento,

            -- Condições
            dim_condicoes_atribuidas.condicoes,

            -- Prescricoes
            dim_prescricoes_atribuidas.prescricoes,

            -- Estabelecimento
            dim_estabelecimento.estabelecimento,

            -- Profissional
            (
                select as struct
                    dim_profissional.id as id,
                    dim_profissional.cpf as cpf,
                    dim_profissional.cns as cns,
                    {{ proper_br("dim_profissional.nome") }} as nome,
                    safe_cast(
                        case
                            when cbo_descricao_profissional like '%Médic%'
                            then 'Médico(a)'
                            when cbo_descricao_profissional like '%Enferm%'
                            then 'Enfermeiro(a)'
                            when cbo_descricao_profissional like '%dentista%'
                            then 'Dentista'
                            when cbo_descricao_profissional like '%social%'
                            then 'Assistente Social'
                            else cbo_descricao_profissional
                        end as string
                    ) as especialidade
            ) as profissional_saude_responsavel,

            -- Prontuário
            struct(
                atendimento.gid as id_atendimento, 'vitacare' as fornecedor
            ) as prontuario,

            -- Metadados
            struct(
                updated_at, loaded_at as imported_at, current_datetime() as processed_at
            ) as metadados,

            atendimento.data_particao,
            safe_cast(dim_paciente.paciente.cpf as int64) as cpf_particao,

        from bruto_atendimento as atendimento
        left join dim_paciente on atendimento.cpf = dim_paciente.pk
        left join
            dim_estabelecimento on atendimento.cnes_unidade = dim_estabelecimento.pk
        left join dim_profissional on atendimento.cns_profissional = dim_profissional.pk
        left join
            dim_condicoes_atribuidas
            on atendimento.gid = dim_condicoes_atribuidas.fk_atendimento
        left join
            dim_prescricoes_atribuidas
            on atendimento.gid = dim_prescricoes_atribuidas.fk_atendimento
    ),

    episodios_validos as (select * from fato_atendimento where id is not null)

-- -=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--
-- Finalização
-- -=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--
select *
from episodios_validos

{% if is_incremental() -%}
    where data_particao in ({{ partitions_to_replace | join(",") }})
{% endif %}
