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
        FROM {{ source('brutos_vitacare_historic_staging', 'PACIENTES') }} 
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

            {{ remove_double_quotes('ap') }} AS ap,
            {{ remove_double_quotes('unidade') }} AS unidade,
            {{ remove_double_quotes('ut_id') }} AS ut_id,
            {{ remove_double_quotes('nome') }} AS nome,
            {{ remove_double_quotes('cns') }} AS cns,
            {{ remove_double_quotes('cpf') }} AS cpf,
            {{ process_null(remove_double_quotes('nis')) }} AS nis,
            {{ remove_double_quotes('npront') }} AS npront,
            {{ remove_double_quotes('sexo') }} AS sexo,
            SAFE_CAST({{ remove_double_quotes('dta_nasc') }} AS DATE) AS dta_nasc,
            {{ remove_double_quotes('code') }} AS code,
            {{ remove_double_quotes('cadastropermanente') }} AS cadastropermanente,
            SAFE_CAST({{ remove_double_quotes('dataatualizacaocadastro') }} AS DATETIME) AS dataatualizacaocadastro,
            SAFE_CAST({{ remove_double_quotes('dataatualizacaovinculoequipe') }} AS DATETIME) AS dataatualizacaovinculoequipe,
            SAFE_CAST({{ remove_double_quotes('datacadastro') }} AS DATETIME) AS datacadastro,
            {{ remove_double_quotes('obito') }} AS obito,
            {{ process_null(remove_double_quotes('dnv')) }} AS dnv,
            {{ process_null(remove_double_quotes('email')) }} AS email,
            {{ process_null(remove_double_quotes('telefone')) }} AS telefone,
            {{ remove_double_quotes('situacaousuario') }} AS situacaousuario,
            {{ process_null(remove_double_quotes('situacaofamiliar')) }} AS situacaofamiliar,
            {{ process_null(remove_double_quotes('racacor')) }} AS racacor,
            {{ process_null(remove_double_quotes('religiao')) }} AS religiao,
            {{ process_null(remove_double_quotes('situacaoprofissional')) }} AS situacaoprofissional,
            {{ process_null(remove_double_quotes('nomesocial')) }} AS nomesocial,
            SAFE_CAST({{ process_null(remove_double_quotes('frequentaescola')) }} AS BOOLEAN) AS frequentaescola,
            {{ process_null(remove_double_quotes('nomemae')) }} AS nomemae,
            {{ process_null(remove_double_quotes('nomepai')) }} AS nomepai,
            SAFE_CAST({{ process_null(remove_double_quotes('membrocomunidadetradicional')) }} AS BOOLEAN) AS membrocomunidadetradicional,
            {{ process_null(remove_double_quotes('ocupacao')) }} AS ocupacao,
            {{ process_null(remove_double_quotes('orientacaosexual')) }} AS orientacaosexual,
            {{ process_null(remove_double_quotes('nacionalidade')) }} AS nacionalidade,
            {{ process_null(remove_double_quotes('paisnascimento')) }} AS paisnascimento,
            SAFE_CAST({{ process_null(remove_double_quotes('participagrupocomunitario')) }} AS BOOLEAN) AS participagrupocomunitario,
            SAFE_CAST({{ process_null(remove_double_quotes('possuiplanosaude')) }} AS BOOLEAN) AS possuiplanosaude,
            {{ process_null(remove_double_quotes('relacaoresponsavelfamiliar')) }} AS relacaoresponsavelfamiliar,
            SAFE_CAST({{ process_null(remove_double_quotes('territoriosocial')) }} AS BOOLEAN) AS territoriosocial,
            {{ process_null(remove_double_quotes('escolaridade')) }} AS escolaridade,
            {{ process_null(remove_double_quotes('identidadegenero')) }} AS identidadegenero,
            SAFE_CAST({{ process_null(remove_double_quotes('criancamatriculadacrechepreescola')) }} AS BOOLEAN) AS criancamatriculadacrechepreescola,
            SAFE_CAST({{ process_null(remove_double_quotes('emsituacaoderua')) }} AS BOOLEAN) AS emsituacaoderua,
            {{ process_null(remove_double_quotes('doencascondicoes')) }} AS doencascondicoes,
            {{ process_null(remove_double_quotes('estadonascimento')) }} AS estadonascimento,
            {{ process_null(remove_double_quotes('estadoresidencia')) }} AS estadoresidencia,
            {{ process_null(remove_double_quotes('municipionascimento')) }} AS municipionascimento,
            {{ process_null(remove_double_quotes('municipioresidencia')) }} AS municipioresidencia,
            {{ process_null(remove_double_quotes('abastecimentoagua')) }} AS abastecimentoagua,
            {{ process_null(remove_double_quotes('animaisnodomicilio')) }} AS animaisnodomicilio,
            {{ process_null(remove_double_quotes('bairro')) }} AS bairro,
            {{ process_null(remove_double_quotes('cep')) }} AS cep,
            {{ process_null(remove_double_quotes('comodos')) }} AS comodos,
            {{ process_null(remove_double_quotes('destinolixo')) }} AS destinolixo,
            {{ process_null(remove_double_quotes('esgotamentosanitario')) }} AS esgotamentosanitario,
            {{ process_null(remove_double_quotes('familiabeneficiariaauxiliobrasil')) }} AS familiabeneficiariaauxiliobrasil,
            {{ process_null(remove_double_quotes('familiabeneficiariacfc')) }} AS familiabeneficiariacfc,
            {{ process_null(remove_double_quotes('logradouro')) }} AS logradouro,
            {{ process_null(remove_double_quotes('luzeletrica')) }} AS luzeletrica,
            {{ process_null(remove_double_quotes('meioscomunicacao')) }} AS meioscomunicacao,
            {{ process_null(remove_double_quotes('meiostransporte')) }} AS meiostransporte,
            {{ process_null(remove_double_quotes('possuifiltroagua')) }} AS possuifiltroagua,
            {{ process_null(remove_double_quotes('rendafamiliar')) }} AS rendafamiliar,
            {{ process_null(remove_double_quotes('responsavelfamiliar')) }} AS responsavelfamiliar,
            {{ process_null(remove_double_quotes('situacaomoradiaposse')) }} AS situacaomoradiaposse,
            {{ process_null(remove_double_quotes('tipodomicilio')) }} AS tipodomicilio,
            {{ process_null(remove_double_quotes('tipologradouro')) }} AS tipologradouro,
            {{ process_null(remove_double_quotes('tratamentoagua')) }} AS tratamentoagua,
            {{ process_null(remove_double_quotes('emcasodoencaprocura')) }} AS emcasodoencaprocura,
            {{ process_null(remove_double_quotes('tempomoradia')) }} AS tempomoradia,
            {{ process_null(remove_double_quotes('familialocalizacao')) }} AS familialocalizacao,
            {{ process_null(remove_double_quotes('codigoequipe')) }} AS codigoequipe,
            {{ process_null(remove_double_quotes('equipe')) }} AS equipe,
            {{ process_null(remove_double_quotes('ineequipe')) }} AS ineequipe,
            {{ process_null(remove_double_quotes('microarea')) }} AS microarea,
            {{ process_null(remove_double_quotes('vulnerabilidadesocial')) }} AS vulnerabilidadesocial,
            {{ remove_double_quotes('updated_at') }} AS updated_at,

            {{ remove_double_quotes('extracted_at') }} AS extracted_at

        FROM pacientes_deduplicados
    )

SELECT
    *
FROM fato_pacientes