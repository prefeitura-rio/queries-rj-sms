{{
    config(
        enabled=true,
        alias="indicadores_recentes_paciente",
    )
}}

-- Consulta para extrair o último IMC, peso, altura por CPF, cruzando com o nome do paciente
WITH 
  episodios AS (
    SELECT * 
    FROM {{ ref('mart_historico_clinico__episodio') }}
  ),

  cids_relevantes AS (
    SELECT 
      paciente.cpf AS cpf_paciente, -- CPF do paciente
      ARRAY_AGG(DISTINCT condicoes.id) AS cids_ativos -- Lista de CIDs ativos do paciente
    FROM
      episodios,
      UNNEST(condicoes) AS condicoes
    WHERE 
      condicoes.situacao = 'ATIVO' -- Considera apenas CIDs ativos
    GROUP BY 
      paciente.cpf
  ),

  ultimo_imc_por_paciente AS (
    SELECT 
      paciente.cpf AS cpf_paciente, -- CPF do paciente
      MAX(data_particao) AS ultima_data_imc, -- Última data do registro de medidas
      ANY_VALUE(medidas.imc) AS ultimo_imc, -- Último IMC registrado (associado à data mais recente)
      ANY_VALUE(medidas.peso) AS ultimo_peso, -- Último peso registrado
      ANY_VALUE(medidas.altura) AS ultima_altura, -- Última altura registrada
      ANY_VALUE(estabelecimento.nome) AS unidade_saude, -- Nome do estabelecimento (unidade de saúde)
      ANY_VALUE(estabelecimento.estabelecimento_tipo) AS tipo_unidade -- Tipo do estabelecimento
    FROM 
      episodios
    WHERE 
      medidas.imc IS NOT NULL -- Apenas registros com IMC preenchido
    GROUP BY 
      paciente.cpf
  ),

  dados_paciente AS (
    SELECT 
      imc_data.cpf_paciente, -- CPF do paciente
      p.dados.nome AS nome_paciente, -- Nome do paciente (dentro de dados.nome)
      imc_data.ultimo_imc AS indice_massa_corporal, -- Último IMC registrado
      imc_data.ultimo_peso AS peso_kg, -- Último peso registrado
      imc_data.ultima_altura AS altura_cm, -- Última altura registrada
      imc_data.ultima_data_imc AS data_ultima_medicao, -- Data do último registro de medidas
      imc_data.unidade_saude AS unidade_de_saude, -- Nome da unidade de saúde
      imc_data.tipo_unidade AS tipo_da_unidade, -- Tipo da unidade de saúde

      -- Indicadores para cada condição (1 = diagnóstico ativo, 0 = sem diagnóstico)
      IF(
        EXISTS (
          SELECT 1 FROM UNNEST(cids_relevantes.cids_ativos) AS cid
          WHERE cid LIKE 'E66%' -- Obesidade
        ), 1, 0
      ) AS obesidade,

      IF(
        EXISTS (
          SELECT 1 FROM UNNEST(cids_relevantes.cids_ativos) AS cid
          WHERE cid BETWEEN 'E10' AND 'E14' -- Diabetes
        ), 1, 0
      ) AS diabetes,

      IF(
        EXISTS (
          SELECT 1 FROM UNNEST(cids_relevantes.cids_ativos) AS cid
          WHERE cid BETWEEN 'I20' AND 'I25' -- Doença Arterial Coronariana
        ), 1, 0
      ) AS doenca_arterial_coronariana,

      IF(
        EXISTS (
          SELECT 1 FROM UNNEST(cids_relevantes.cids_ativos) AS cid
          WHERE cid BETWEEN 'M15' AND 'M19' -- Osteoartrite
        ), 1, 0
      ) AS osteoartrite,

      IF(
        EXISTS (
          SELECT 1 FROM UNNEST(cids_relevantes.cids_ativos) AS cid
          WHERE cid = 'M17' -- Artrite de Joelhos
        ), 1, 0
      ) AS artrite_joelhos,

      IF(
        EXISTS (
          SELECT 1 FROM UNNEST(cids_relevantes.cids_ativos) AS cid
          WHERE cid = 'G47.3' -- Apneia Obstrutiva do Sono
        ), 1, 0
      ) AS apneia_obstrutiva_sono

    FROM 
      ultimo_imc_por_paciente imc_data
    LEFT JOIN 
      cids_relevantes
      ON imc_data.cpf_paciente = cids_relevantes.cpf_paciente
    LEFT JOIN 
      {{ ref('mart_historico_clinico__paciente') }} p
      ON imc_data.cpf_paciente = p.cpf -- Vinculação correta pelo campo CPF
  )

SELECT * 
FROM dados_paciente
where {{ validate_cpf("cpf_paciente") }}