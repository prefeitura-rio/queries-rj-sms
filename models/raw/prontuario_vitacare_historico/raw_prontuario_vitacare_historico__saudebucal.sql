{{
    config(
        alias="saude_bucal", 
        materialized="incremental",
        schema="brutos_prontuario_vitacare_historico",
        partition_by={
            "field": "data_particao",
            "data_type": "date",
            "granularity": "day"
        }
    )
}}

with

    source_saude_bucal as (
        select 
            concat(
                nullif(id_cnes, ''), 
                '.',
                nullif(replace(acto_id, '.0', ''), '')
            ) as id_prontuario_global,
            *
        from {{ source('brutos_prontuario_vitacare_historico_staging', 'saude_bucal') }} 
    ),


      -- Using window function to deduplicate saude_bucal
    saude_bucal_deduplicados as (
        select
            *
        from source_saude_bucal 
        qualify row_number() over (partition by id_prontuario_global order by extracted_at desc) = 1 
    ),

    fato_saude_bucal as (
        select
            -- PKs e Chaves
            id_prontuario_global,
            replace(acto_id, '.0', '') as id_prontuario_local,
            id_cnes,

            {{ process_null('maincomplaint') }} as queixa_principal,
            {{ process_null('mucosallesions') }} as lesoes_na_mucosa,
            {{ process_null('mucosallesionsdescription') }} as lesoes_na_mucosa_descricao,
            {{ process_null('fluorosis') }} as fluorose,
            {{ process_null('enamelalterations') }} as alteracoes_do_esmalte,
            {{ process_null('tmj') }} as articulacao_temporomandibular,
            {{ process_null('occlusion') }} as oclusao,
            {{ process_null('congenitalanomaly') }} as anomalia_congenita,
            {{ process_null('harmfulhabits') }} as habitos_prejudiciais,
            {{ process_null('prosthesisneed') }} as necessidade_de_protese,
            {{ process_null('cariesactivity') }} as atividade_de_carie,
            safe_cast({{ process_null('lastdentalvisit') }} as datetime) as ultima_visita_ao_dentista,
            {{ process_null('clinicalassessmentobservations') }} as observacoes_da_avaliacao_clinica,
            {{ process_null('demandtype') }} as tipo_de_demanda,
            {{ process_null('oralhealthsurveillance') }} as vigilancia_em_saude_bucal,
            {{ process_null('treatmenttype') }} as tipo_de_tratamento,
            {{ process_null('individualsuppliesprovided') }} as suprimentos_individuais_fornecidos,
            {{ process_null('treatmentdischarge') }} as alta_do_tratamento,
            safe_cast({{ process_null('dischargedate') }} as datetime) as data_da_alta,
            {{ process_null('dischargetype') }} as tipo_de_alta,
            replace({{ process_null('numberofteethintreatment') }}, '.0', '') as numero_de_dentes_em_tratamento,
            replace({{ process_null('periodontaldiseaseactivity') }}, '.0', '') as atividade_de_doenca_periodontal,
            {{ process_null('assessment') }} as avaliacao,
            {{ process_null('assessmentobservations') }} as observacoes_da_avaliacao,
            {{ process_null('proceduresperformed') }} as procedimentos_realizados,
            {{ process_null('assessmentfollowing1716') }} as avaliacao_seguinte_1716,
            {{ process_null('assessmentfollowing11') }} as avaliacao_seguinte_11,
            {{ process_null('assessmentfollowing2627') }} as avaliacao_seguinte_2627,
            {{ process_null('assessmentfollowing3637') }} as avaliacao_seguinte_3637,
            {{ process_null('assessmentfollowing31') }} as avaliacao_seguinte_31,
            {{ process_null('assessmentfollowing4746') }} as avaliacao_seguinte_4746,
            {{ process_null('dentalprosthesisuse') }} as uso_de_protese_dentaria,
            {{ process_null('referralspecialty') }} as encaminhamento_especialidade,

            safe_cast(extracted_at as datetime) AS loaded_at,
            date(safe_cast(extracted_at as datetime)) as data_particao
        from saude_bucal_deduplicados
    )

select
    *
from fato_saude_bucal