{{
    config(
        alias="hanseniase", 
        materialized="incremental",
        unique_key = 'id_prontuario_global',
        cluster_by= 'id_prontuario_global',
        schema="brutos_prontuario_vitacare_historico",
        partition_by={
            "field": "data_particao",
            "data_type": "date",
            "granularity": "day"
        }
    )
}}

{% set last_partition = get_last_partition_date(this) %}

WITH

    source_hanseniase AS (
        SELECT 
            CONCAT(
                NULLIF(id_cnes, ''), 
                '.',
                NULLIF(REPLACE(acto_id, '.0', ''), '')
            ) as id_prontuario_global,
            *
        FROM {{ source('brutos_prontuario_vitacare_historico_staging', 'hanseniase') }} 
        {% if is_incremental() %}
            WHERE data_particao > '{{last_partition}}'
        {% endif %}
    ),


      -- Using window function to deduplicate hanseniase
    hanseniase_deduplicados AS (
        SELECT
            *
        FROM source_hanseniase 
        qualify row_number() over (partition by id_prontuario_global order by extracted_at desc) = 1 
    ),

    fato_hanseniase AS (
        SELECT
            -- PKs e Chaves
            id_prontuario_global,
            REPLACE(acto_id, '.0', '') as id_prontuario_local,
            id_cnes,

            safe_cast({{ process_null('datanotificacao') }} as datetime) as data_notificacao,
            replace({{ process_null('numlesoescut') }}, '.0', '') as num_lesoes_cut,
            {{ process_null('formaclinica') }} as forma_clinica,
            {{ process_null('classificacaooperacional') }} as classificacao_operacional,
            replace({{ process_null('numnervosafetados') }}, '.0', '') as num_nervos_afetados,
            {{ process_null('avaliacaograuincapacidade') }} as avaliacao_grau_incapacidade,
            {{ process_null('modoentrada') }} as modo_entrada,
            {{ process_null('baciloscopia') }} as baciloscopia,
            safe_cast({{ process_null('dataexamehistopatologico') }} as datetime) as data_exame_histopatologico,
            {{ process_null('resultadoexamehistopatologico') }} as resultado_exame_histopatologico,
            safe_cast({{ process_null('datainiciotratamento') }} as datetime) as data_inicio_tratamento,
            replace({{ process_null('idadeiniciotratamento') }}, '.0', '') as idade_inicio_tratamento,
            {{ process_null('esquematerapeutico') }} as esquema_terapeutico,
            {{ process_null('historiaclinicadoencaobservacoes') }} as historia_clinica_doenca_observacoes,
            {{ process_null('incapacidadeolho') }} as incapacidade_olho,
            {{ process_null('incapacidademao') }} as incapacidade_mao,
            {{ process_null('incapacidadepe') }} as incapacidade_pe,
            {{ process_null('maiorgrauavaliadotratamento') }} as maior_grau_avaliado_tratamento,
            {{ process_null('comprometimentolaringeo') }} as comprometimento_laringeo,
            {{ process_null('desabamentonasal') }} as desabamento_nasal,
            {{ process_null('paralisiafacial') }} as paralisia_facial,
            {{ process_null('observacoesnotasgerais') }} as observacoes_notas_gerais,
            {{ process_null('examecontatosresultado') }} as exame_contatos_resultado,
            {{ process_null('dadosacompanhamentocondicao') }} as dados_acompanhamento_condicao,
            {{ process_null('dadosacompanhamentoobservacoes') }} as dados_acompanhamento_observacoes,
            safe_cast({{ process_null('dataexclusao') }} as datetime) as data_exclusao,
            replace({{ process_null('idadenaexclusao') }}, '.0', '') as idade_na_exclusao,
            {{ process_null('motivoexclusao') }} as motivo_exclusao,
            {{ process_null('numerosinanhansen') }} as numero_sinan_hansen,
            {{ process_null('hanseniasecomunicantesreferidos') }} as hanseniase_comunicantes_referidos,

            extracted_at AS loaded_at,
            DATE(SAFE_CAST(extracted_at AS DATETIME)) AS data_particao
        FROM hanseniase_deduplicados
    )

SELECT
    *
FROM fato_hanseniase