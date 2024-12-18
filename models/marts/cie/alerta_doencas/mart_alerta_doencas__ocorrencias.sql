{{
    config(
        schema="projeto_alertas_doencas",
        alias="ocorrencias",
        materialized="incremental",
        incremental_strategy="insert_overwrite",
        partition_by={
            "field": "data_particao",
            "data_type": "date",
            "granularity": "day",
        },
    )
}}

-- Tabela auxiliar dos CIDs de atenção
with
    source as (
        select *
        from {{ source("brutos_prontuario_vitacare_staging", "atendimento_eventos") }}
        {% if is_incremental() %}
            where data_particao = cast(current_date('America/Sao_Paulo') as string)
        {% endif %}
    ),

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

    -- Consulta a tabela
    -- rj-sms.brutos_prontuario_vitacare_staging._atendimento_eventos e
    -- join com a tabela auxiliar
    final as (
        select
            patient_cpf as paciente_cpf,
            patient_code,
            source_id,
            data__unidade_cnes as unidade_cnes,
            data__unidade_ap as unidade_ap,
            data__profissional__equipe__nome as equipe_nome,
            data__profissional__equipe__cod_ine as equipe_cod_ine,
            data__profissional__equipe__cod_equipe as equipe_cod,
            safe_cast(data__datahora_inicio_atendimento as timestamp) as data_inicio,
            data__tipo_consulta as tipo_consulta,
            data__soap_subjetivo_motivo as soap_subjetivo_motivo,
            json_value(condicoes, '$.cod_cid10') as cid,
            json_value(condicoes, '$.estado') as estado_cid,
            safe_cast(
                json_value(condicoes, '$.data_diagnostico') as timestamp
            ) as data_diagnostico,
            aps_cid_atencao.agravo,
            safe_cast(
                safe_cast(data__datahora_fim_atendimento as datetime) as date
            ) as data_particao

        from source, unnest(json_query_array(data__condicoes)) as condicoes
        left join
            cid10_atencao aps_cid_atencao
            on regexp_contains(
                json_value(condicoes, '$.cod_cid10'), aps_cid_atencao.cod
            )
        where
            json_value(condicoes, '$.estado') in ('ATIVO', 'N.E')
            and aps_cid_atencao.cod is not null
    )

select *
from final
{% if is_incremental() %}
    where data_particao = current_date('America/Sao_Paulo')
{% endif %}
