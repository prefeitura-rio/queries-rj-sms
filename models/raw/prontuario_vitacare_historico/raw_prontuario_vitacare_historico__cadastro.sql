{{
    config(
        alias="cadastro",
        materialized="incremental",
        schema="brutos_prontuario_vitacare_historico",
        partition_by={
            "field": "data_particao",
            "data_type": "date",
            "granularity": "day"
        }
    )
}}

WITH

    source_pacientes AS (
        SELECT
            *
        FROM {{ source('brutos_prontuario_vitacare_historico_staging', 'cadastro') }}
    ),


      -- Using window function to deduplicate pacientes
    pacientes_deduplicados AS (
        SELECT
            *
        FROM (
            SELECT
                *,
                ROW_NUMBER() OVER (PARTITION BY cpf, id_cnes ORDER BY extracted_at DESC) AS rn
            FROM source_pacientes
        )
        WHERE rn = 1
    ),

    fato_pacientes AS (
        SELECT
            -- PKs e Chaves

            id_cnes, 

            {{ process_null('ap') }} AS ap,
            {{ process_null('unidade') }} AS unidade,
            {{ process_null('ut_id') }} AS ut_id,
            {{ process_null(proper_br('nome')) }} AS nome,
            {{ process_null('cns') }} AS cns,
            {{ process_null('cpf') }} AS cpf,
            {{ process_null('nis') }} AS nis,
            {{ process_null('npront') }} AS npront,
            case 
                when sexo='female' then 'Feminino'
                when sexo='male' then 'Masculino'
                else null
            end as sexo,
            SAFE_CAST({{ process_null('dta_nasc') }} AS DATE) AS dta_nasc,
            {{ process_null('code') }} AS code,
            cadastropermanente = '1' AS cadastropermanente,
            SAFE_CAST(dataatualizacaocadastro AS DATETIME) AS dataatualizacaocadastro,
            SAFE_CAST({{ process_null('dataatualizacaovinculoequipe') }} AS DATETIME) AS dataatualizacaovinculoequipe,
            SAFE_CAST({{ process_null('datacadastro') }} AS DATETIME) AS datacadastro,
            CASE
                WHEN obito = '1' THEN TRUE
                WHEN obito = '0' THEN FALSE
                ELSE NULL
            END AS obito,
            {{ process_null('dnv') }} AS dnv,
            {{ process_null('email') }} AS email,
            {{ process_null('telefone') }} AS telefone,
            {{ process_null('situacaousuario') }} AS situacaousuario,
            {{ process_null('situacaofamiliar') }} AS situacaofamiliar,
            {{ process_null('racacor') }} AS racacor,
            {{ process_null('religiao') }} AS religiao,
            {{ process_null('situacaoprofissional') }} AS situacaoprofissional,
            {{ process_null(proper_br('nomesocial')) }} AS nomesocial,
            CASE
                WHEN frequentaescola = '1' THEN TRUE
                WHEN frequentaescola = '0' THEN FALSE
                ELSE NULL
            END AS frequentaescola,
            {{ process_null(proper_br('nomemae')) }} AS nomemae,
            {{ process_null(proper_br('nomepai')) }} AS nomepai,
            CASE
                WHEN membrocomunidadetradicional = '1' THEN TRUE
                WHEN membrocomunidadetradicional = '0' THEN FALSE
                ELSE NULL
            END AS membrocomunidadetradicional,
            {{ process_null('ocupacao') }} AS ocupacao,
            {{ process_null('orientacaosexual') }} AS orientacaosexual,
            {{ process_null('nacionalidade') }} AS nacionalidade,
            {{ process_null('paisnascimento') }} AS paisnascimento,
            participagrupocomunitario = '1' AS participagrupocomunitario,
            CASE
                WHEN possuiplanosaude = '1' THEN TRUE
                WHEN possuiplanosaude = '0' THEN FALSE
                ELSE NULL
            END AS possuiplanosaude,
            {{ process_null('relacaoresponsavelfamiliar') }} AS relacaoresponsavelfamiliar,
            CASE
                WHEN territoriosocial = '1' THEN TRUE
                WHEN territoriosocial = '0' THEN FALSE
                ELSE NULL
            END AS territoriosocial,
            {{ process_null('escolaridade') }} AS escolaridade,
            {{ process_null('identidadegenero') }} AS identidadegenero,
            CASE
                WHEN criancamatriculadacrechepreescola = '1' THEN TRUE
                WHEN criancamatriculadacrechepreescola = '0' THEN FALSE
                ELSE NULL
            END AS criancamatriculadacrechepreescola,
            CASE
                WHEN emsituacaoderua = '1' THEN TRUE
                WHEN emsituacaoderua = '0' THEN FALSE
                ELSE NULL
            END AS emsituacaoderua,
            {{ process_null('doencascondicoes') }} AS doencascondicoes,
            {{ process_null('estadonascimento') }} AS estadonascimento,
            {{ process_null('estadoresidencia') }} AS estadoresidencia,
            {{ process_null('municipionascimento') }} AS municipionascimento,
            {{ process_null('municipioresidencia') }} AS municipioresidencia,
            {{ process_null('abastecimentoagua') }} AS abastecimentoagua,
            CASE
                WHEN animaisnodomicilio = '1' THEN TRUE
                WHEN animaisnodomicilio = '0' THEN FALSE
                ELSE NULL
            END AS animaisnodomicilio,
            {{ process_null('bairro') }} AS bairro,
            {{ process_null('cep') }} AS cep,
            {{ process_null('comodos') }} AS comodos,
            {{ process_null('destinolixo') }} AS destinolixo,
            {{ process_null('esgotamentosanitario') }} AS esgotamentosanitario,
            CASE
                WHEN familiabeneficiariaauxiliobrasil = '1' THEN TRUE
                WHEN familiabeneficiariaauxiliobrasil = '0' THEN FALSE
                ELSE NULL
            END AS familiabeneficiariaauxiliobrasil,
            CASE
                WHEN familiabeneficiariacfc = '1' THEN TRUE
                WHEN familiabeneficiariacfc = '0' THEN FALSE
                ELSE NULL
            END AS familiabeneficiariacfc,
            {{ process_null('logradouro') }} AS logradouro,
            CASE
                WHEN luzeletrica = '1' THEN TRUE
                WHEN luzeletrica = '0' THEN FALSE
                ELSE NULL
            END AS luzeletrica,
            {{ process_null('meioscomunicacao') }} AS meioscomunicacao,
            {{ process_null('meiostransporte') }} AS meiostransporte,
            CASE
                WHEN possuifiltroagua = '1' THEN TRUE
                WHEN possuifiltroagua = '0' THEN FALSE
                ELSE NULL
            END AS possuifiltroagua,
            {{ process_null('rendafamiliar') }} AS rendafamiliar,
            CASE
                WHEN responsavelfamiliar = '1' THEN TRUE
                WHEN responsavelfamiliar = '0' THEN FALSE
                ELSE NULL
            END AS responsavelfamiliar,
            {{ process_null('situacaomoradiaposse') }} AS situacaomoradiaposse,
            {{ process_null('tipodomicilio') }} AS tipodomicilio,
            {{ process_null('tipologradouro') }} AS tipologradouro,
            {{ process_null('tratamentoagua') }} AS tratamentoagua,
            {{ process_null('emcasodoencaprocura') }} AS emcasodoencaprocura,
            {{ process_null('tempomoradia') }} AS tempomoradia,
            {{ process_null('familialocalizacao') }} AS familialocalizacao,
            {{ process_null('codigoequipe') }} AS codigoequipe,
            {{ process_null('equipe') }} AS equipe,
            {{ process_null('ineequipe') }} AS ineequipe,
            {{ process_null('microarea') }} AS microarea,
            CASE
                WHEN vulnerabilidadesocial = '1' THEN TRUE
                WHEN vulnerabilidadesocial = '0' THEN FALSE
                ELSE NULL
            END AS vulnerabilidadesocial,
            SAFE_CAST(updated_at AS DATETIME) AS updated_at,

            extracted_at AS loaded_at,
            DATE(SAFE_CAST(extracted_at AS DATETIME)) AS data_particao
        FROM pacientes_deduplicados
    ),

    -- Filtro temporário para remover registros anteriores à carga oficial (24/06/2025 17:15)
    fato_filtrado AS (
        SELECT *
        FROM fato_pacientes
        WHERE PARSE_TIMESTAMP('%F %H:%M:%E6S', loaded_at) > TIMESTAMP('2025-06-24 17:15:00.000000')
    )

SELECT
    *
FROM fato_filtrado