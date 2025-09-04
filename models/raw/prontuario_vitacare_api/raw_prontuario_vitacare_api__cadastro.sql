{{ config(
    alias="cadastro",
    materialized="incremental",
    schema="brutos_prontuario_vitacare_api",
    incremental_strategy="insert_overwrite",
    partition_by={"field": "data_particao", "data_type": "date", "granularity": "day"}
) }}

{% set last_partition = get_last_partition_date(this) %}

WITH
  bruto_atendimento AS (
    SELECT
      CAST(CONCAT(NULLIF(payload_cnes,''), '.', NULLIF(source_id,'')) AS STRING) AS id_prontuario_global,
      patient_cpf AS cpf,
      source_updated_at,
      SAFE_CAST(datalake_loaded_at AS DATETIME) AS loaded_at,
      data
    FROM {{ source('brutos_prontuario_vitacare_api_staging', 'paciente_continuo') }} AS src
    {% if is_incremental() %}
        WHERE DATE(datalake_loaded_at, 'America/Sao_Paulo') >= DATE('{{ last_partition }}')
    {% endif %}
    QUALIFY ROW_NUMBER() OVER (
      PARTITION BY id_prontuario_global
      ORDER BY loaded_at DESC
    ) = 1
  ),

  fato_api AS (
    SELECT
      {{ process_null("JSON_EXTRACT_SCALAR(data, '$.cnes')") }}                              AS id_cnes,
      {{ process_null("REGEXP_REPLACE(JSON_EXTRACT_SCALAR(data, '$.ap'), '\\\\.0$', '')") }}   AS ap,
      {{ process_null("JSON_EXTRACT_SCALAR(data, '$.unidade')") }}                         AS unidade,
      {{ process_null("JSON_EXTRACT_SCALAR(data, '$.id')") }}                              AS ut_id,
      {{ process_null("INITCAP(JSON_EXTRACT_SCALAR(data, '$.nome'))") }}                   AS nome,
      {{ process_null("JSON_EXTRACT_SCALAR(data, '$.cns')") }}                            AS cns,
      cpf,
      {{ process_null("NULLIF(JSON_EXTRACT_SCALAR(data, '$.nis'), '')") }}               AS nis,
      {{ process_null("JSON_EXTRACT_SCALAR(data, '$.nPront')") }}                        AS npront,
      CASE
        WHEN LOWER(JSON_EXTRACT_SCALAR(data, '$.sexo')) = 'male'   THEN 'Masculino'
        WHEN LOWER(JSON_EXTRACT_SCALAR(data, '$.sexo')) = 'female' THEN 'Feminino'
        ELSE NULL
      END                                                                                 AS sexo,
      SAFE_CAST(
        {{ process_null("JSON_EXTRACT_SCALAR(data, '$.dataNascimento')") }}
        AS DATE
      )                                                                                   AS data_nascimento,
      JSON_EXTRACT_SCALAR(data, '$.cpf') || '.' ||
        REPLACE(JSON_EXTRACT_SCALAR(data, '$.dataNascimento'), '-', '')                    AS code,
      CASE
        WHEN JSON_EXTRACT_SCALAR(data, '$.cadastroPermanente') = 'true'  THEN TRUE
        WHEN JSON_EXTRACT_SCALAR(data, '$.cadastroPermanente') = 'false' THEN FALSE
        ELSE NULL
      END                                                                                 AS cadastro_permanente,
      SAFE_CAST(
        {{ process_null("JSON_EXTRACT_SCALAR(data, '$.dataAtualizacaoCadastro')") }}
        AS DATETIME
      )                                                                                   AS data_atualizacao_cadastro,
      SAFE_CAST(
        {{ process_null("JSON_EXTRACT_SCALAR(data, '$.dataAtualizacaoVinculoEquipe')") }}
        AS DATETIME
      )                                                                                   AS data_atualizacao_vinculo_equipe,
      SAFE_CAST(
        {{ process_null("JSON_EXTRACT_SCALAR(data, '$.dataCadastro')") }}
        AS DATETIME
      )                                                                                   AS data_cadastro,
      CASE
        WHEN JSON_EXTRACT_SCALAR(data, '$.obito') = 'true'  THEN TRUE
        WHEN JSON_EXTRACT_SCALAR(data, '$.obito') = 'false' THEN FALSE
        ELSE NULL
      END                                                                                 AS obito,
      {{ process_null("JSON_EXTRACT_SCALAR(data, '$.dnv')") }}                            AS dnv,
      {{ process_null("JSON_EXTRACT_SCALAR(data, '$.email')") }}                          AS email,
      {{ process_null("JSON_EXTRACT_SCALAR(data, '$.telefone')") }}                       AS telefone,
      {{ process_null("JSON_EXTRACT_SCALAR(data, '$.situacaoUsuario')") }}                AS situacao_usuario,
      {{ process_null("JSON_EXTRACT_SCALAR(data, '$.situacaoFamiliar')") }}               AS situacao_familiar,
      {{ process_null("JSON_EXTRACT_SCALAR(data, '$.racaCor')") }}                        AS raca_cor,
      {{ process_null("JSON_EXTRACT_SCALAR(data, '$.religiao')") }}                       AS religiao,
      {{ process_null("JSON_EXTRACT_SCALAR(data, '$.situacaoProfissional')") }}           AS situacao_profissional,
      {{ process_null("INITCAP(JSON_EXTRACT_SCALAR(data, '$.nomeSocial'))") }}             AS nome_social,
      CASE
        WHEN JSON_EXTRACT_SCALAR(data, '$.frequentaEscola') = 'true'  THEN TRUE
        WHEN JSON_EXTRACT_SCALAR(data, '$.frequentaEscola') = 'false' THEN FALSE
        ELSE NULL
      END                                                                                 AS frequenta_escola,
      {{ process_null("INITCAP(JSON_EXTRACT_SCALAR(data, '$.nomeMae'))") }}                AS nome_mae,
      {{ process_null("INITCAP(JSON_EXTRACT_SCALAR(data, '$.nomePai'))") }}                AS nome_pai,

      CASE
        WHEN JSON_EXTRACT_SCALAR(data, '$.membroComunidadeTradicional') = 'true' THEN TRUE
        WHEN JSON_EXTRACT_SCALAR(data, '$.membroComunidadeTradicional') = 'false' THEN FALSE
        ELSE NULL
      END AS membro_comunidade_tradicional,

      {{ process_null("JSON_EXTRACT_SCALAR(data, '$.ocupacao')") }}                       AS ocupacao,
      {{ process_null("JSON_EXTRACT_SCALAR(data, '$.orientacaoSexual')") }}               AS orientacao_sexual,
      {{ process_null("JSON_EXTRACT_SCALAR(data, '$.nacionalidade')") }}                  AS nacionalidade,
      {{ process_null("JSON_EXTRACT_SCALAR(data, '$.paisNascimento')") }}                 AS pais_nascimento,
      CASE
        WHEN JSON_EXTRACT_SCALAR(data, '$.participaGrupoComunitario') = 'true'  THEN TRUE
        WHEN JSON_EXTRACT_SCALAR(data, '$.participaGrupoComunitario') = 'false' THEN FALSE
        ELSE NULL
      END                                                                                 AS participa_grupo_comunitario,
      CASE
        WHEN JSON_EXTRACT_SCALAR(data, '$.possuiPlanoSaude') = 'true'  THEN TRUE
        WHEN JSON_EXTRACT_SCALAR(data, '$.possuiPlanoSaude') = 'false' THEN FALSE
        ELSE NULL
      END                                                                                 AS possui_plano_saude,
      {{ process_null("JSON_EXTRACT_SCALAR(data, '$.relacaoResponsavelFamiliar')") }}      AS relacao_responsavel_familiar,
      {{ process_null("JSON_EXTRACT_SCALAR(data, '$.territorioSocial')") }}               AS territorio_social,
      {{ process_null("JSON_EXTRACT_SCALAR(data, '$.escolaridade')") }}                   AS escolaridade,
      {{ process_null("JSON_EXTRACT_SCALAR(data, '$.identidadeGenero')") }}               AS identidade_genero,
      CASE
        WHEN JSON_EXTRACT_SCALAR(data, '$.criancaMatriculadaCrechePreEscola') = 'true'  THEN TRUE
        WHEN JSON_EXTRACT_SCALAR(data, '$.criancaMatriculadaCrechePreEscola') = 'false' THEN FALSE
        ELSE NULL
      END                                                                                 AS crianca_matriculada_creche_pre_escola,
      CASE
        WHEN JSON_EXTRACT_SCALAR(data, '$.emSituacaoDeRua') = 'true'  THEN TRUE
        WHEN JSON_EXTRACT_SCALAR(data, '$.emSituacaoDeRua') = 'false' THEN FALSE
        ELSE NULL
      END                                                                                 AS em_situacao_de_rua,
      {{ process_null("ARRAY_TO_STRING(JSON_EXTRACT_ARRAY(data, '$.doencasCondicoes'), ', ')") }} AS doencas_condicoes,
      {{ process_null("JSON_EXTRACT_SCALAR(data, '$.estadoNascimento')") }}              AS estado_nascimento,
      {{ process_null("JSON_EXTRACT_SCALAR(data, '$.estadoResidencia')") }}              AS estado_residencia,
      {{ process_null("JSON_EXTRACT_SCALAR(data, '$.municipioNascimento')") }}           AS municipio_nascimento,
      {{ process_null("JSON_EXTRACT_SCALAR(data, '$.municipioResidencia')") }}           AS municipio_residencia,
      {{ process_null("JSON_EXTRACT_SCALAR(data, '$.abastecimentoAgua')") }}             AS abastecimento_agua,
      CASE
        WHEN JSON_EXTRACT_SCALAR(data, '$.animaisNoDomicilio') = 'true' THEN TRUE
        WHEN JSON_EXTRACT_SCALAR(data, '$.animaisNoDomicilio') = 'false' THEN FALSE
        ELSE NULL
      END AS animais_no_domicilio,
      {{ process_null("JSON_EXTRACT_SCALAR(data, '$.bairro')") }}                         AS bairro,
      {{ process_null("JSON_EXTRACT_SCALAR(data, '$.cep')") }}                            AS cep,
      {{ process_null("JSON_EXTRACT_SCALAR(data, '$.comodos')") }}                        AS comodos,
      {{ process_null("JSON_EXTRACT_SCALAR(data, '$.destinoLixo')") }}                    AS destino_lixo,
      CASE
        WHEN JSON_EXTRACT_SCALAR(data, '$.esgotamentoSanitario') = 'true'  THEN TRUE
        WHEN JSON_EXTRACT_SCALAR(data, '$.esgotamentoSanitario') = 'false' THEN FALSE
        ELSE NULL
      END                                                                                 AS esgotamento_sanitario,
      CASE
        WHEN JSON_EXTRACT_SCALAR(data, '$.familiaBeneficiariaAuxilioBrasil') = 'true'  THEN TRUE
        WHEN JSON_EXTRACT_SCALAR(data, '$.familiaBeneficiariaAuxilioBrasil') = 'false' THEN FALSE
        ELSE NULL
      END                                                                                 AS familia_beneficiaria_auxilio_brasil,
      CASE
        WHEN JSON_EXTRACT_SCALAR(data, '$.familiaBeneficiariaCfc') = 'true'  THEN TRUE
        WHEN JSON_EXTRACT_SCALAR(data, '$.familiaBeneficiariaCfc') = 'false' THEN FALSE
        ELSE NULL
      END                                                                                 AS familia_beneficiaria_cfc,
      {{ process_null("JSON_EXTRACT_SCALAR(data, '$.logradouro')") }}                     AS logradouro,
      CASE
        WHEN JSON_EXTRACT_SCALAR(data, '$.luzEletrica') = 'true'  THEN TRUE
        WHEN JSON_EXTRACT_SCALAR(data, '$.luzEletrica') = 'false' THEN FALSE
        ELSE NULL
      END                                                                                 AS luz_eletrica,
      {{ process_null("ARRAY_TO_STRING(JSON_EXTRACT_ARRAY(data, '$.meiosComunicacao'), ', ')") }}    AS meios_comunicacao,
      {{ process_null("ARRAY_TO_STRING(JSON_EXTRACT_ARRAY(data, '$.meiosTransporte'), ', ')") }}     AS meios_transporte,
      {{ process_null("JSON_EXTRACT_SCALAR(data, '$.possuiFiltroAgua')") }}               AS possui_filtro_agua,
      {{ process_null("JSON_EXTRACT_SCALAR(data, '$.rendaFamiliar')") }}                  AS renda_familiar,
      {{ process_null("JSON_EXTRACT_SCALAR(data, '$.responsavelFamiliar')") }}            AS responsavel_familiar,
      {{ process_null("JSON_EXTRACT_SCALAR(data, '$.situacaoMoradiaPosse')") }}           AS situacao_moradia_posse,
      {{ process_null("JSON_EXTRACT_SCALAR(data, '$.tipoDomicilio')") }}                  AS tipo_domicilio,
      {{ process_null("JSON_EXTRACT_SCALAR(data, '$.tipoLogradouro')") }}                 AS tipo_logradouro,
      {{ process_null("JSON_EXTRACT_SCALAR(data, '$.tratamentoAgua')") }}                  AS tratamento_agua,
      {{ process_null("ARRAY_TO_STRING(JSON_EXTRACT_ARRAY(data, '$.emCasoDoencaProcura'), ', ')") }} AS em_caso_doenca_procura,
      {{ process_null("JSON_EXTRACT_SCALAR(data, '$.tempoMoradia')") }}                    AS tempo_moradia,
      {{ process_null("JSON_EXTRACT_SCALAR(data, '$.familiaLocalizacao')") }}              AS familia_localizacao,
      {{ process_null("JSON_EXTRACT_SCALAR(data, '$.codigoEquipe')") }}                   AS codigo_equipe,
      {{ process_null("JSON_EXTRACT_SCALAR(data, '$.equipe')") }}                          AS equipe,
      {{ process_null("JSON_EXTRACT_SCALAR(data, '$.ineEquipe')") }}                       AS ine_equipe,
      {{ process_null("JSON_EXTRACT_SCALAR(data, '$.microarea')") }}                       AS microarea,
      CASE
        WHEN JSON_EXTRACT_SCALAR(data, '$.vulnerabilidadeSocial') = 'true'  THEN TRUE
        WHEN JSON_EXTRACT_SCALAR(data, '$.vulnerabilidadeSocial') = 'false' THEN FALSE
        ELSE NULL
      END                                                                                 AS vulnerabilidade_social,
      SAFE_CAST(source_updated_at    AS DATETIME)                                         AS updated_at,
      loaded_at,
      date(loaded_at) as data_particao
    FROM bruto_atendimento
  ),

  pacientes_deduplicados AS (
    SELECT *
    FROM (
      SELECT
        fato_api.*,                     
        ROW_NUMBER() OVER (
          PARTITION BY cpf, id_cnes
          ORDER BY data_atualizacao_cadastro DESC,
                  cadastro_permanente       DESC
        ) AS rn
      FROM fato_api
    )
    WHERE rn = 1                   
  )


SELECT
  * EXCEPT (rn)                    
FROM pacientes_deduplicados
