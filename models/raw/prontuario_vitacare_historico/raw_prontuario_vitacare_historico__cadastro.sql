{{
    config(
        schema="brutos_prontuario_vitacare_historico",
        alias="cadastro",
        materialized="incremental",
        incremental_strategy='merge', 
        unique_key=['cpf', 'id_cnes'],
        partition_by={
            "field": "data_particao",
            "data_type": "date",
            "granularity": "day"
        },
    )
}}

WITH

    source_pacientes AS (
        SELECT
            *
        FROM {{ source('brutos_prontuario_vitacare_historico_staging', 'cadastro') }}
        {% if is_incremental() %}
            WHERE DATE(extracted_at) > (SELECT MAX(data_particao) FROM {{ this }})
        {% endif %}
    ),


      -- Using window function to deduplicate pacientes
    pacientes_deduplicados AS (
        SELECT
            *
        FROM (
            SELECT
                *,
                ROW_NUMBER() OVER (PARTITION BY cpf, id_cnes ORDER BY dataatualizacaocadastro DESC, cadastropermanente DESC) AS rn
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
            SAFE_CAST({{ process_null('dta_nasc') }} AS DATE) AS data_nascimento,
            {{ process_null('code') }} AS code,
            cadastropermanente = '1' AS cadastro_permanente, 
            SAFE_CAST(dataatualizacaocadastro AS DATETIME) AS data_atualizacao_cadastro, 
            SAFE_CAST({{ process_null('dataatualizacaovinculoequipe') }} AS DATETIME) AS data_atualizacao_vinculo_equipe, 
            SAFE_CAST({{ process_null('datacadastro') }} AS DATETIME) AS data_cadastro, 
            CASE
                WHEN obito = '1' THEN TRUE
                WHEN obito = '0' THEN FALSE
                ELSE NULL
            END AS obito,
            {{ process_null('dnv') }} AS dnv,
            {{ process_null('email') }} AS email,
            {{ process_null('telefone') }} AS telefone,
            {{ process_null('situacaousuario') }} AS situacao_usuario, 
            {{ process_null('situacaofamiliar') }} AS situacao_familiar, 
            {{ process_null('racacor') }} AS raca_cor, 
            {{ process_null('religiao') }} AS religiao,
            {{ process_null('situacaoprofissional') }} AS situacao_profissional, 
            {{ process_null(proper_br('nomesocial')) }} AS nome_social, 
            CASE
                WHEN frequentaescola = '1' THEN TRUE
                WHEN frequentaescola = '0' THEN FALSE
                ELSE NULL
            END AS frequenta_escola, 
            {{ process_null(proper_br('nomemae')) }} AS nome_mae, 
            {{ process_null(proper_br('nomepai')) }} AS nome_pai, 
            CASE
                WHEN membrocomunidadetradicional = '1' THEN TRUE
                WHEN membrocomunidadetradicional = '0' THEN FALSE
                ELSE NULL
            END AS membro_comunidade_tradicional, 
            {{ process_null('ocupacao') }} AS ocupacao,
            {{ process_null('orientacaosexual') }} AS orientacao_sexual, 
            {{ process_null('nacionalidade') }} AS nacionalidade,
            {{ process_null('paisnascimento') }} AS pais_nascimento, 
            participagrupocomunitario = '1' AS participa_grupo_comunitario, 
            CASE
                WHEN possuiplanosaude = '1' THEN TRUE
                WHEN possuiplanosaude = '0' THEN FALSE
                ELSE NULL
            END AS possui_plano_saude, 
            {{ process_null('relacaoresponsavelfamiliar') }} AS relacao_responsavel_familiar, 
            CASE
                WHEN territoriosocial = '1' THEN TRUE
                WHEN territoriosocial = '0' THEN FALSE
                ELSE NULL
            END AS territorio_social, 
            {{ process_null('escolaridade') }} AS escolaridade,
            {{ process_null('identidadegenero') }} AS identidade_genero, 
            CASE
                WHEN criancamatriculadacrechepreescola = '1' THEN TRUE
                WHEN criancamatriculadacrechepreescola = '0' THEN FALSE
                ELSE NULL
            END AS crianca_matriculada_creche_pre_escola, 
            CASE
                WHEN emsituacaoderua = '1' THEN TRUE
                WHEN emsituacaoderua = '0' THEN FALSE
                ELSE NULL
            END AS em_situacao_de_rua, 
            {{ process_null('doencascondicoes') }} AS doencas_condicoes, 
            {{ process_null('estadonascimento') }} AS estado_nascimento, 
            {{ process_null('estadoresidencia') }} AS estado_residencia, 
            {{ process_null('municipionascimento') }} AS municipio_nascimento, 
            {{ process_null('municipioresidencia') }} AS municipio_residencia, 
            {{ process_null('abastecimentoagua') }} AS abastecimento_agua, 
            CASE
                WHEN animaisnodomicilio = '1' THEN TRUE
                WHEN animaisnodomicilio = '0' THEN FALSE
                ELSE NULL
            END AS animais_no_domicilio, 
            {{ process_null('bairro') }} AS bairro,
            {{ process_null('cep') }} AS cep,
            {{ process_null('comodos') }} AS comodos,
            {{ process_null('destinolixo') }} AS destino_lixo, 
            {{ process_null('esgotamentosanitario') }} AS esgotamento_sanitario, 
            CASE
                WHEN familiabeneficiariaauxiliobrasil = '1' THEN TRUE
                WHEN familiabeneficiariaauxiliobrasil = '0' THEN FALSE
                ELSE NULL
            END AS familia_beneficiaria_auxilio_brasil, 
            CASE
                WHEN familiabeneficiariacfc = '1' THEN TRUE
                WHEN familiabeneficiariacfc = '0' THEN FALSE
                ELSE NULL
            END AS familia_beneficiaria_cfc, 
            {{ process_null('logradouro') }} AS logradouro,
            CASE
                WHEN luzeletrica = '1' THEN TRUE
                WHEN luzeletrica = '0' THEN FALSE
                ELSE NULL
            END AS luz_eletrica, 
            {{ process_null('meioscomunicacao') }} AS meios_comunicacao, 
            {{ process_null('meiostransporte') }} AS meios_transporte, 
            CASE
                WHEN possuifiltroagua = '1' THEN TRUE
                WHEN possuifiltroagua = '0' THEN FALSE
                ELSE NULL
            END AS possui_filtro_agua, 
            {{ process_null('rendafamiliar') }} AS renda_familiar, 
            CASE
                WHEN responsavelfamiliar = '1' THEN TRUE
                WHEN responsavelfamiliar = '0' THEN FALSE
                ELSE NULL
            END AS responsavel_familiar, 
            {{ process_null('situacaomoradiaposse') }} AS situacao_moradia_posse, 
            {{ process_null('tipodomicilio') }} AS tipo_domicilio, 
            {{ process_null('tipologradouro') }} AS tipo_logradouro, 
            {{ process_null('tratamentoagua') }} AS tratamento_agua, 
            {{ process_null('emcasodoencaprocura') }} AS em_caso_doenca_procura, 
            {{ process_null('tempomoradia') }} AS tempo_moradia, 
            {{ process_null('familialocalizacao') }} AS familia_localizacao, 
            {{ process_null('codigoequipe') }} AS codigo_equipe, 
            {{ process_null('equipe') }} AS equipe,
            {{ process_null('ineequipe') }} AS ine_equipe, 
            {{ process_null('microarea') }} AS microarea,
            CASE
                WHEN vulnerabilidadesocial = '1' THEN TRUE
                WHEN vulnerabilidadesocial = '0' THEN FALSE
                ELSE NULL
            END AS vulnerabilidade_social, 
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