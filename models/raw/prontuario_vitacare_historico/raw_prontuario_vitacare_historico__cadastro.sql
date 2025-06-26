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
            {{ process_null('nome') }} AS nome,
            {{ process_null(proper_br('cns')) }} AS cns,
            {{ process_null('cpf') }} AS cpf,
            {{ process_null('nis') }} AS nis,
            {{ process_null('npront') }} AS npront,
            sexo AS sexo,
            SAFE_CAST({{ process_null('dta_nasc') }} AS DATE) AS dta_nasc,
            {{ process_null('code') }} AS code,
            SAFE_CAST(cadastropermanente AS INT) AS cadastropermanente,
            SAFE_CAST(dataatualizacaocadastro AS DATETIME) AS dataatualizacaocadastro,
            SAFE_CAST({{ process_null('dataatualizacaovinculoequipe') }} AS DATETIME) AS dataatualizacaovinculoequipe,
            SAFE_CAST({{ process_null('datacadastro') }} AS DATETIME) AS datacadastro,
            SAFE_CAST(obito AS INT) AS obito,
            dnv AS dnv,
            email AS email,
            telefone AS telefone,
            situacaousuario AS situacaousuario,
            situacaofamiliar AS situacaofamiliar,
            racacor AS racacor,
            religiao AS religiao,
            situacaoprofissional AS situacaoprofissional,
            {{ process_null('nomesocial') }} AS nomesocial,
            frequentaescola AS frequentaescola,
            nomemae AS nomemae,
            nomepai AS nomepai,
            membrocomunidadetradicional AS membrocomunidadetradicional,
            ocupacao AS ocupacao,
            orientacaosexual AS orientacaosexual,
            {{ process_null('nacionalidade') }} AS nacionalidade,
            {{ process_null('paisnascimento') }} AS paisnascimento,
            SAFE_CAST(participagrupocomunitario AS INT) AS participagrupocomunitario,
            possuiplanosaude AS possuiplanosaude,
            relacaoresponsavelfamiliar AS relacaoresponsavelfamiliar,
            territoriosocial AS territoriosocial,
            escolaridade AS escolaridade,
            identidadegenero AS identidadegenero,
            criancamatriculadacrechepreescola AS criancamatriculadacrechepreescola,
            SAFE_CAST(emsituacaoderua AS INT) AS emsituacaoderua,
            doencascondicoes AS doencascondicoes,
            estadonascimento AS estadonascimento,
            estadoresidencia AS estadoresidencia,
            municipionascimento AS municipionascimento,
            municipioresidencia AS municipioresidencia,
            abastecimentoagua AS abastecimentoagua,
            animaisnodomicilio AS animaisnodomicilio,
            bairro AS bairro,
            cep AS cep,
            comodos AS comodos,
            destinolixo AS destinolixo,
            esgotamentosanitario AS esgotamentosanitario,
            familiabeneficiariaauxiliobrasil AS familiabeneficiariaauxiliobrasil,
            familiabeneficiariacfc AS familiabeneficiariacfc,
            logradouro AS logradouro,
            luzeletrica AS luzeletrica,
            meioscomunicacao AS meioscomunicacao,
            meiostransporte AS meiostransporte,
            possuifiltroagua AS possuifiltroagua,
            rendafamiliar AS rendafamiliar,
            SAFE_CAST(responsavelfamiliar AS INT) AS responsavelfamiliar,
            situacaomoradiaposse AS situacaomoradiaposse,
            tipodomicilio AS tipodomicilio,
            tipologradouro AS tipologradouro,
            tratamentoagua AS tratamentoagua,
            emcasodoencaprocura AS emcasodoencaprocura,
            tempomoradia AS tempomoradia,
            familialocalizacao AS familialocalizacao,
            codigoequipe AS codigoequipe,
            equipe AS equipe,
            ineequipe AS ineequipe,
            microarea AS microarea,
            SAFE_CAST(vulnerabilidadesocial AS INT) AS vulnerabilidadesocial,
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