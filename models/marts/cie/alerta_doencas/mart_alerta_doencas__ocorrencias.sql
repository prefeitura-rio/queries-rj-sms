{{
    config(
        schema="projeto_alerta_doencas",
        alias="ocorrencias",
        materialized="table",
        partition_by={
            "field": "data_particao",
            "data_type": "date",
            "granularity": "day",
        },
    )
}}


with
    -- Ocorrencias (episódios)
    ocorrencias_legado as (
        select
            patient_cpf as cpf,
            payload_cnes as cnes,
            data__unidade_ap as ap,
            safe_cast(data__datahora_inicio_atendimento as datetime) as datahora_inicio,
            safe_cast(data__datahora_fim_atendimento as datetime) as datahora_fim,
            data__profissional__equipe__cod_equipe as cod_equipe_profissional,
            data__profissional__equipe__cod_ine as cod_ine_equipe_profissional,
            data__profissional__equipe__nome as nome_equipe_profissional,
            data__tipo_consulta as tipo,
            data__condicoes as condicoes,
            data__soap_subjetivo_motivo as soap_subjetivo_motivo,
            concat(nullif(payload_cnes, ''), '.', nullif(source_id, '')) as id_episodio,
            safe_cast(datalake_loaded_at as timestamp) as datalake_loaded_at
        from {{ source("brutos_prontuario_vitacare_staging", "atendimento_eventos_cloned") }}
    ),

    ocorrencias_continuo as (
        select
            patient_cpf as cpf,
            payload_cnes as cnes,
            json_extract_scalar(data, "$.unidade_ap") as ap,
            safe_cast(
                {{ process_null("json_extract_scalar(data, '$.datahora_inicio_atendimento')") }} as datetime
            ) as datahora_inicio,
            safe_cast(
                {{ process_null("json_extract_scalar(data, '$.datahora_fim_atendimento')") }} as datetime
            ) as datahora_fim,
            json_extract_scalar(data, "$.profissional.equipe.cod_equipe") as cod_equipe_profissional,
            json_extract_scalar(data, "$.profissional.equipe.cod_ine") as cod_ine_equipe_profissional,
            json_extract_scalar(data, "$.profissional.equipe.nome") as nome_equipe_profissional,

            json_extract_scalar(data, "$.tipo_consulta") as tipo,
            json_extract(data, "$.condicoes") as condicoes,
            json_extract_scalar(data, "$.soap_subjetivo_motivo") as soap_subjetivo_motivo,
            concat(nullif(payload_cnes, ''), '.', nullif(source_id, '')) as id_episodio,
            safe_cast(datalake_loaded_at as timestamp) as datalake_loaded_at
        from {{ source("brutos_prontuario_vitacare_staging", "atendimento_continuo") }}
    ),

    ocorrencias_deduplicadas as (
        select *
        from ocorrencias_continuo
        union all
        select *
        from ocorrencias_legado
        qualify
            row_number() over (
                partition by id_episodio order by datalake_loaded_at desc
            )
            = 1
    ),

    -- Dimensão de estabelecimentos
    estabelecimentos as (select * from {{ ref("dim_estabelecimento") }}),

    -- Dimensão de profissionais
    equipes as (select * from {{ ref("dim_equipe") }}),

    -- Dimensão de pacientes
    pacientes as (select * from {{ ref("raw_prontuario_vitacare__paciente") }}),

    -- Tabela auxiliar dos CIDs de atenção
    cid10_atencao as (
        select 'A33' as cod, 'tetano_recem_nascido_neonatal' as agravo
        union all
        select 'A34', 'tetano_obstetrico'
        union all
        select 'A35', 'outros_tipos_tetano'
        union all
        select 'A77', 'febre_maculosa_rickettsioses'
        union all
        select 'B50', 'malaria_plasmodium_falciparum'
        union all
        select 'B51', 'malaria_plasmodium_vivax'
        union all
        select 'B52', 'malaria_plasmodium_malariae'
        union all
        select 'B53', 'outras_formas_malaria'
        union all
        select 'B54', 'malaria_nao_especificada'
        union all
        select 'A00', 'colera'
        union all
        select 'A010', 'febre_tifoide'
        union all
        select 'A051', 'botulismo'
        union all
        select 'A379', 'coqueluche_nao_especificada'
        union all
        select 'A370', 'coqueluche_bordetella_pertussis'
        union all
        select 'A809', 'poliomielite_aguda_nao_especificada'
        union all
        select 'A829', 'raiva_nao_especificada'
        union all
        select 'A923', 'infeccao_virus_west_nile'
        union all
        select 'A959', 'febre_amarela_nao_especificada'
        union all
        select 'A988', 'outras_febres_hemorragicas_virus'
        union all
        select 'B550', 'leishmaniose_visceral'
        union all
        select 'B551', 'leishmaniose_cutanea'
        union all
        select 'B659', 'esquistossomose_nao_especificada'
        union all
        select 'P350', 'sindrome_rubeola_congenita'
        union all
        select 'B05', 'sarampo'
        union all
        select 'A930', 'febre_oropouche'
        union all
        select 'A83', 'encefalite_virus_transmitidos_por_mosquitos'
        union all
        select 'B06', 'rubeola'
    ),

    -- - Tabela final
    final as (
        select
            id_episodio,

            -- Paciente
            ocorrencias_deduplicadas.cpf as paciente_cpf,
            pacientes.nome as paciente_nome,
            pacientes.data_nascimento as paciente_data_nascimento,
            pacientes.sexo as paciente_sexo,
            pacientes.telefone as paciente_telefone,

            -- Estabelecimento
            cnes as unidade_cnes,
            estabelecimentos.nome_limpo as unidade_nome,
            ap as unidade_ap,
            estabelecimentos.telefone as unidade_telefone,

            -- Equipe
            nome_equipe_profissional as equipe_nome,
            cod_ine_equipe_profissional as equipe_cod_ine,
            cod_equipe_profissional as equipe_cod,
            equipes.telefone as equipe_whatsapp,

            -- Atendimento
            ocorrencias_deduplicadas.datahora_inicio as data_inicio,
            ocorrencias_deduplicadas.tipo as tipo_consulta,
            ocorrencias_deduplicadas.soap_subjetivo_motivo as soap_subjetivo_motivo,

            -- Condição
            json_value(condicoes, '$.cod_cid10') as cid,
            json_value(condicoes, '$.estado') as estado_cid,
            safe_cast(
                json_value(condicoes, '$.data_diagnostico') as timestamp
            ) as data_diagnostico,
            aps_cid_atencao.agravo,

            -- Metadados
            datalake_loaded_at,
            safe_cast(
                safe_cast(datahora_fim as datetime) as date
            ) as data_particao

        from
            ocorrencias_deduplicadas,
            unnest(json_query_array(condicoes)) as condicoes
        left join
            cid10_atencao aps_cid_atencao
            on regexp_contains(
                json_value(condicoes, '$.cod_cid10'), aps_cid_atencao.cod
            )
        left join
            estabelecimentos
            on ocorrencias_deduplicadas.cnes = estabelecimentos.id_cnes
        left join
            equipes
            on ocorrencias_deduplicadas.cod_ine_equipe_profissional
            = equipes.id_ine
        left join
            pacientes
            on ocorrencias_deduplicadas.cpf = pacientes.cpf
            and ocorrencias_deduplicadas.cnes = pacientes.id_cnes
        where
            json_value(condicoes, '$.estado') in ('ATIVO', 'N.E')
            and aps_cid_atencao.cod is not null
    )

select *
from final
{% if is_incremental() %}
    where data_particao >= date_sub(current_date('America/Sao_Paulo'), interval 2 day)
{% endif %}
