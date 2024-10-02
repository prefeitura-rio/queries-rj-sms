{{
    config(
        schema="saude_cnes",
        alias="habilitacao_sus_rio_historico"
    )
}}

with 
versao_atual as (
    select MAX(data_particao) as versao 
    from {{ ref("raw_cnes_web__tipo_unidade") }}
),

estabelecimentos_mrj_sus as (
    select * from {{ ref("dim_estabelecimento_sus_rio_historico") }} where safe_cast(data_particao as string) = (select versao from versao_atual)
),

habilitacoes_non_unique as (
  select
    ano,
    mes,
    id_estabelecimento_cnes as id_cnes,
    tipo_habilitacao as id_habilitacao,
    nivel_habilitacao,
    case
      when ano_competencia_final = 9999 then 1
      else 0
    end as habilitacao_ativa_indicador,
    ano_competencia_inicial as habilitacao_ano_inicio,
    mes_competencia_inicial as habilitacao_mes_inicio,
    case
      when ano_competencia_final = 9999 then NULL
      else ano_competencia_final
    end as habilitacao_ano_fim,
    case
      when mes_competencia_final = 99 then NULL
      else mes_competencia_final
    end as habilitacao_mes_fim
  from
    {{ref("raw_cnes_ftp__habilitacao")}}
  where ano >= 2010 and safe_cast(id_estabelecimento_cnes as int64) in (select distinct safe_cast(id_cnes as int64) from estabelecimentos_mrj_sus)
),

habilitacoes as (
  select distinct * from habilitacoes_non_unique
),

habilitacoes_mapping_cnesweb AS (
  SELECT
    id_habilitacao,
    habilitacao,
    tipo_origem,
    tipo_habilitacao
  FROM 
    {{ ref("raw_cnes_web__tipo_habilitacao") }}
  WHERE
    data_particao = (SELECT versao FROM versao_atual)

    -- removendo ids ambiguos (não unicos).. são poucos
    AND id_habilitacao NOT IN (
      SELECT
        id_habilitacao
      FROM (
        SELECT
          id_habilitacao,
          COUNT(*) AS contagem
        FROM
          {{ ref("raw_cnes_web__tipo_habilitacao") }}
        WHERE
          data_particao = (SELECT versao FROM versao_atual)
        GROUP BY
          id_habilitacao
        HAVING
          contagem > 1
      )
    )
),

final as (
    select
        struct (
            data_atualizao_registro,
            usuario_atualizador_registro,
            mes_particao,
            ano_particao,
            data_particao,
            data_carga,
            data_snapshot
        ) as metadados,

        struct (
            -- Identificação
            estabs.id_cnes,
            id_unidade,
            nome_razao_social,
            nome_fantasia,
            nome_limpo,
            nome_sigla,
            nome_complemento,
            cnpj_mantenedora,

            -- Responsabilização
            esfera,
            id_natureza_juridica,
            natureza_juridica_descr,
            tipo_gestao,
            tipo_gestao_descr,
            responsavel_sms,
            administracao,
            diretor_clinico_cpf,
            diretor_clinico_conselho,

            -- Atributos
            tipo_turno,
            turno_atendimento,
            aberto_sempre,

            -- Tipagem dos Estabelecimentos (CNES)
            id_tipo_unidade,
            tipo,

            -- Tipagem dos Estabelecimentos (DIT)
            tipo_sms,
            tipo_sms_simplificado,
            tipo_sms_agrupado,

            -- Tipagem dos Estabelecimentos (SUBGERAL)
            tipo_unidade_alternativo,
            tipo_unidade_agrupado,

            -- Localização
            id_ap,
            ap,
            endereco_cep,
            endereco_bairro,
            endereco_logradouro,
            endereco_numero,
            endereco_complemento,
            endereco_latitude,
            endereco_longitude,

            -- Status
            ativa,

            -- Prontuário
            prontuario_tem,
            prontuario_versao,
            prontuario_estoque_tem_dado,
            prontuario_estoque_motivo_sem_dado,

            -- Informações de contato
            telefone,
            email,
            facebook,
            instagram,
            twitter,

            -- Indicadores
            estabelecimento_sms_indicador,
            vinculo_sus_indicador,
            atendimento_internacao_sus_indicador,	
            atendimento_ambulatorial_sus_indicador,
            atendimento_sadt_sus_indicador,
            atendimento_urgencia_sus_indicador,  
            atendimento_outros_sus_indicador, 
            atendimento_vigilancia_sus_indicador,
            atendimento_regulacao_sus_indicador
        ) as estabelecimentos,

        struct (
            estabs.ano,
            estabs.mes,
            hab.id_habilitacao,
            habilitacao,
            habilitacao_ativa_indicador,
            nivel_habilitacao,
            tipo_origem,
            tipo_habilitacao,
            habilitacao_ano_inicio,
            habilitacao_mes_inicio,
            habilitacao_ano_fim,
            habilitacao_mes_fim
        ) as habilitacoes
        
    from habilitacoes as hab
    left join habilitacoes_mapping_cnesweb as map on safe_cast(hab.id_habilitacao as int64) = safe_cast(map.id_habilitacao as int64)
    left join estabelecimentos_mrj_sus as estabs on (hab.ano = estabs.ano and hab.mes = estabs.mes and safe_cast(hab.id_cnes as int64) = safe_cast(estabs.id_cnes as int64))
)

select * from final