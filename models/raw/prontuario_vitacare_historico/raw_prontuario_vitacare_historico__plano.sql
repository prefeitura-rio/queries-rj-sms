{{
    config(
        alias="plano", 
        materialized="table",
        schema="brutos_prontuario_vitacare_historico",
        partition_by={
            "field": "data_particao",
            "data_type": "date",
            "granularity": "day"
        }
    )
}}

with

    source_plano as (
        select 
            concat(
                nullif(id_cnes, ''), 
                '.',
                nullif(replace(acto_id, '.0', ''), '')
            ) as id_prontuario_global,
            *
        from {{ source('brutos_prontuario_vitacare_historico_staging', 'plano') }} 
    ),


      -- Using window function to deduplicate plano
    plano_deduplicados as (
        select
            *
        from source_plano 
        qualify row_number() over (partition by id_prontuario_global order by extracted_at desc) = 1 
    ),

    fato_plano as (
        select
            -- PKs e Chaves
            id_prontuario_global,
            replace(acto_id, '.0', '') as id_prontuario_local,
            id_cnes,

            {{ process_null('planobservations') }} as observacoes_plano,
            {{ process_null('prescriptionselection') }} as selecao_da_prescricao,
            {{ process_null('prescribeditems') }} as itens_prescritos,
            {{ process_null('freetextprescription') }} as prescricao_em_texto_livre,
            {{ process_null('prescriptionentitysub') }} as sub_entidade_da_prescricao,
            {{ process_null('sportspracticerecommendationaccarioca') }} as recomendacao_de_pratica_esportiva_ac_carioca,
            {{ process_null('materialcollection') }} as coleta_de_material,
            {{ process_null('entryreasonaccarioca') }} as motivo_da_entrada_ac_carioca,
            {{ process_null('clinicalindicationaccarioca') }} as indicacao_clinica_ac_carioca,
            {{ process_null('riskfactorsaccarioca') }} as fatores_de_risco_ac_carioca,
            {{ process_null('clinicalfollowupaccarioca') }} as acompanhamento_clinico_ac_carioca,
            {{ process_null('clinicalfollowupdetailsaccarioca') }} as detalhes_do_acompanhamento_clinico_ac_carioca,
            {{ process_null('specialtyreferrals') }} as encaminhamentos_para_especialidade,
            {{ process_null('clinicalprocedures') }} as procedimentos_clinicos,
            {{ process_null('plasterremovalobservations') }} as observacoes_da_remocao_de_gesso,
            {{ process_null('alcoholdetoxobservations') }} as observacoes_da_desintoxicacao_de_alcool,
            {{ process_null('cantoplastynailremovalobservations') }} as observacoes_da_cantoplastia_remocao_de_unha,
            {{ process_null('nebulizationobservations') }} as observacoes_da_nebulizacao,
            {{ process_null('sutureobservations') }} as observacoes_da_sutura,
            {{ process_null('iudinsertionobservations') }} as observacoes_da_insercao_de_diu,
            {{ process_null('cerumenremovalobservations') }} as observacoes_da_remocao_de_cerume,
            {{ process_null('abscessdrainageobservations') }} as observacoes_da_drenagem_de_abscesso,

            safe_cast(extracted_at as datetime) AS loaded_at,
            date(safe_cast(extracted_at as datetime)) as data_particao
        from plano_deduplicados
    )

select
    *
from fato_plano