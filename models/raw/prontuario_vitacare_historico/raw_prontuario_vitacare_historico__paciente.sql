{{
    config(
        alias="pacientes",
        materialized="table",
        schema="brutos_prontuario_vitacare_historico",
    )
}}

WITH

    source_pacientes AS (
        SELECT
            *
        FROM {{ source('brutos_prontuario_vitacare_historico_staging', 'pacientes') }}
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

            {{ remove_double_quotes('id_cnes') }} AS cnes_unidade,

            {{ process_null(remove_double_quotes('ap')) }} AS ap,
            {{ process_null(remove_double_quotes('unidade')) }} AS unidade,
            {{ process_null(remove_double_quotes('ut_id')) }} AS ut_id,
            {{ process_null(remove_double_quotes('nome')) }} AS nome,
            {{ remove_double_quotes('cns') }} AS cns,
            {{ process_null(remove_double_quotes('cpf')) }} AS cpf,
            {{ remove_double_quotes('nis') }} AS nis,
            {{ remove_double_quotes('npront') }} AS npront,
            {{ remove_double_quotes('sexo') }} AS sexo,
            {{ process_null(SAFE_CAST({{ remove_double_quotes('dta_nasc') }} AS DATE)) }} AS dta_nasc,
            {{ process_null(remove_double_quotes('code')) }} AS code,
            SAFE_CAST({{ remove_double_quotes('cadastropermanente') }} AS INT) AS cadastropermanente,
            SAFE_CAST({{ remove_double_quotes('dataatualizacaocadastro') }} AS DATETIME) AS dataatualizacaocadastro,
            {{ process_null(SAFE_CAST({{ remove_double_quotes('dataatualizacaovinculoequipe') }} AS DATETIME)) }} AS dataatualizacaovinculoequipe,
            {{ process_null(SAFE_CAST({{ remove_double_quotes('datacadastro') }} AS DATETIME)) }} AS datacadastro,
            SAFE_CAST({{ remove_double_quotes('obito') }} AS INT) AS obito,
            {{ remove_double_quotes('dnv') }} AS dnv,
            {{ remove_double_quotes('email') }} AS email,
            {{ remove_double_quotes('telefone') }} AS telefone,
            {{ remove_double_quotes('situacaousuario') }} AS situacaousuario,
            {{ remove_double_quotes('situacaofamiliar') }} AS situacaofamiliar,
            {{ remove_double_quotes('racacor') }} AS racacor,
            {{ remove_double_quotes('religiao') }} AS religiao,
            {{ remove_double_quotes('situacaoprofissional') }} AS situacaoprofissional,
            {{ process_null(remove_double_quotes('nomesocial')) }} AS nomesocial,
            {{ remove_double_quotes('frequentaescola') }} AS frequentaescola,

            {{ remove_double_quotes('nomemae') }} AS nomemae,
            {{ remove_double_quotes('nomepai') }} AS nomepai,
            {{ remove_double_quotes('membrocomunidadetradicional') }} AS membrocomunidadetradicional,
            {{ remove_double_quotes('ocupacao') }} AS ocupacao,
            {{ remove_double_quotes('orientacaosexual') }} AS orientacaosexual,
            {{ process_null(remove_double_quotes('nacionalidade')) }} AS nacionalidade,
            {{ process_null(remove_double_quotes('paisnascimento')) }} AS paisnascimento,
            SAFE_CAST({{ remove_double_quotes('participagrupocomunitario') }} AS INT) AS participagrupocomunitario,
            {{ remove_double_quotes('possuiplanosaude') }} AS possuiplanosaude,
            {{ remove_double_quotes('relacaoresponsavelfamiliar') }} AS relacaoresponsavelfamiliar,
            {{ remove_double_quotes('territoriosocial') }} AS territoriosocial,
            {{ remove_double_quotes('escolaridade') }} AS escolaridade,
            {{ remove_double_quotes('identidadegenero') }} AS identidadegenero,
            {{ remove_double_quotes('criancamatriculadacrechepreescola') }} AS criancamatriculadacrechepreescola,
            SAFE_CAST({{ remove_double_quotes('emsituacaoderua') }} AS INT) AS emsituacaoderua,
            {{ remove_double_quotes('doencascondicoes') }} AS doencascondicoes,
            {{ remove_double_quotes('estadonascimento') }} AS estadonascimento,
            {{ remove_double_quotes('estadoresidencia') }} AS estadoresidencia,
            {{ remove_double_quotes('municipionascimento') }} AS municipionascimento,
            {{ remove_double_quotes('municipioresidencia') }} AS municipioresidencia,
            {{ remove_double_quotes('abastecimentoagua') }} AS abastecimentoagua,
            {{ remove_double_quotes('animaisnodomicilio') }} AS animaisnodomicilio,
            {{ remove_double_quotes('bairro') }} AS bairro,
            {{ remove_double_quotes('cep') }} AS cep,
            {{ remove_double_quotes('comodos') }} AS comodos,
            {{ remove_double_quotes('destinolixo') }} AS destinolixo,
            {{ remove_double_quotes('esgotamentosanitario') }} AS esgotamentosanitario,
            {{ remove_double_quotes('familiabeneficiariaauxiliobrasil') }} AS familiabeneficiariaauxiliobrasil,
            {{ remove_double_quotes('familiabeneficiariacfc') }} AS familiabeneficiariacfc,
            {{ remove_double_quotes('logradouro') }} AS logradouro,
            {{ remove_double_quotes('luzeletrica') }} AS luzeletrica,
            {{ remove_double_quotes('meioscomunicacao') }} AS meioscomunicacao,
            {{ remove_double_quotes('meiostransporte') }} AS meiostransporte,
            {{ remove_double_quotes('possuifiltroagua') }} AS possuifiltroagua,
            {{ remove_double_quotes('rendafamiliar') }} AS rendafamiliar,
            SAFE_CAST({{ remove_double_quotes('responsavelfamiliar') }} AS INT) AS responsavelfamiliar,
            {{ remove_double_quotes('situacaomoradiaposse') }} AS situacaomoradiaposse,
            {{ remove_double_quotes('tipodomicilio') }} AS tipodomicilio,
            {{ remove_double_quotes('tipologradouro') }} AS tipologradouro,
            {{ remove_double_quotes('tratamentoagua') }} AS tratamentoagua,
            {{ remove_double_quotes('emcasodoencaprocura') }} AS emcasodoencaprocura,
            {{ remove_double_quotes('tempomoradia') }} AS tempomoradia,
            {{ remove_double_quotes('familialocalizacao') }} AS familialocalizacao,
            {{ remove_double_quotes('codigoequipe') }} AS codigoequipe,
            {{ remove_double_quotes('equipe') }} AS equipe,
            {{ remove_double_quotes('ineequipe') }} AS ineequipe,
            {{ remove_double_quotes('microarea') }} AS microarea,
            SAFE_CAST({{ remove_double_quotes('vulnerabilidadesocial') }} AS INT) AS vulnerabilidadesocial,
            SAFE_CAST({{ remove_double_quotes('updated_at') }} AS DATETIME) AS updated_at,

            {{ remove_double_quotes('extracted_at') }} AS extracted_at
        FROM pacientes_deduplicados
    )

SELECT
    *
FROM fato_pacientes