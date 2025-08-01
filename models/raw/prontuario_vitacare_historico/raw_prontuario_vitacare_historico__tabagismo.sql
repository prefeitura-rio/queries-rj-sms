{{
    config(
        alias="tabagismo", 
        materialized="table",
        schema="brutos_prontuario_vitacare_historico",
        partition_by={
            "field": "data_particao",
            "data_type": "date",
            "granularity": "day"
        }
    )
}}

WITH

    source_tabagismo AS (
        SELECT 
            CONCAT(
                NULLIF(id_cnes, ''), 
                '.',
                NULLIF(REPLACE(acto_id, '.0', ''), '')
            ) as id_prontuario_global,
            *
        FROM {{ source('brutos_prontuario_vitacare_historico_staging', 'tabagismo') }} 
    ),


      -- Using window function to deduplicate tabagismo
    tabagismo_deduplicados AS (
        SELECT
            *
        FROM source_tabagismo 
        qualify row_number() over (partition by id_prontuario_global order by extracted_at desc) = 1 
    ),

    fato_tabagismo AS (
        SELECT
            -- PKs e Chaves
            id_prontuario_global,
            REPLACE(acto_id, '.0', '') as id_prontuario_local,
            id_cnes,

            safe_cast({{ process_null('cdtpeso') }} as float64) as cdt_peso,
            replace({{ process_null('cdtaltura') }}, '.0', '') as cdt_altura,
            safe_cast({{ process_null('cdtimc') }} as float64) as cdt_imc,
            replace({{ process_null('cdttamax') }}, '.0', '') as cdt_tamax,
            replace({{ process_null('cdttamin') }}, '.0', '') as cdt_tamin,
            {{ process_null('cdtintervencoes') }} as cdt_intervencoes,
            {{ process_null('cdtintervencoesobs') }} as cdt_intervencoesobs,
            {{ process_null('cdtperimabdominal') }} as cdt_perimabdominal,
            {{ process_null('monoxidocarbono') }} as monoxido_carbono,
            {{ process_null('tempoprimcig') }} as tempo_prim_cig,
            case 
                when {{ process_null('fumarlocaisproib') }} = 'Sim' 
                then true
                when {{ process_null('fumarlocaisproib') }} = 'Não' 
                then false
                else NULL
            end as fumar_locais_proib,
            {{ process_null('melhorcigdia') }} as melhor_cig_dia,
            {{ process_null('numcigdia') }} as num_cig_dia,
            case 
                when {{ process_null('fumamanha') }} = 'Sim' 
                then true
                when {{ process_null('fumamanha') }} = 'Não' 
                then false
                else NULL
            end as fuma_manha,
            case 
                when {{ process_null('fumadoente') }} = 'Sim' 
                then true
                when {{ process_null('fumadoente') }} = 'Não' 
                then false
                else NULL
            end as fuma_doente,
            {{ process_null('fagerstrongraudepend') }} as fagerstrom_grau_depend,
            {{ process_null('fumantefasesmotivacionais') }} as fumante_fases_motivacionais,
            {{ process_null('fumantenotasobservacoestxt') }} as fumante_notas_observacoes_txt,
            {{ process_null('tabacotipoutilizado') }} as tabaco_tipo_utilizado,
            {{ process_null('tabacotipoutilizadoqual') }} as tabaco_tipo_utilizado_qual,
            {{ process_null('tabacoparticipoutratamento') }} as tabaco_participou_tratamento,
            {{ process_null('tabacoencontro') }} as tabaco_encontro,
            safe_cast({{ process_null('tabacoencontrodata') }} as datetime) as tabaco_encontro_data,
            {{ process_null('tabacosituacaopacientefase1') }} as tabaco_situacao_paciente_fase1,
            case 
                when {{ process_null('tabacoapoiomedicamento') }} = 'Sim' 
                then true
                when {{ process_null('tabacoapoiomedicamento') }} = 'Não' 
                then false
                else NULL
            end as tabaco_apoio_medicamento,
            safe_cast({{ process_null('tabacoapoiomedicamentodata') }} as datetime) as tabaco_apoio_medicamento_data,
            {{ process_null('tabacotipomedicacao') }} as tabaco_tipo_medicacao,
            {{ process_null('tabacomanutencao') }} as tabaco_manutencao,
            safe_cast({{ process_null('tabacomanutencaodata') }} as datetime) as tabaco_manutencao_data,
            {{ process_null('tabacosituacaopacientefase2') }} as tabaco_situacao_paciente_fase2,
            safe_cast({{ process_null('tabacoapoiomedicamentodatafim') }} as datetime) as tabaco_apoio_medicamento_data_fim,

            extracted_at AS loaded_at,
            DATE(SAFE_CAST(extracted_at AS DATETIME)) AS data_particao
        FROM tabagismo_deduplicados
    )

SELECT
    *
FROM fato_tabagismo